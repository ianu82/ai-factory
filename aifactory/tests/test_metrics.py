"""Hand-built durable histories; no model/provider or GitHub access required."""
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from aifactory.db import session_scope
from aifactory.metrics import workspace_metrics
from aifactory.models import Artifact, Decision, Event, Message, Outcome, Plan, Repository, Run, Task

START = datetime(2025, 1, 1)
MID = START + timedelta(hours=1)
END = START + timedelta(hours=2)


def seed(s, ws, **fields):
    repo = Repository(workspace_id=ws, full_name=f"acme/repo-{s.query(Repository).count()}", config={"checks": ["pytest -q"]})
    s.add(repo)
    s.flush()
    o = Outcome(workspace_id=ws, number=s.query(Outcome).count() + 1, title="Delivery", status="completed",
                created_at=START, completed_at=END)
    s.add(o)
    s.flush()
    p = Plan(outcome_id=o.id, status="approved")
    s.add(p)
    s.flush()
    t = Task(workspace_id=ws, outcome_id=o.id, plan_id=p.id, repo_id=repo.id, key="T1", title="Task",
             acceptance=["works"], status="completed", **fields)
    s.add(t)
    s.flush()
    return o, p, t


def passed(n=1, **extra):
    return {"n": n, "result": "verified", "checks": [{"command": "pytest -q", "ok": True}],
            "verdict": {"verdict": "pass", "criteria": [{"criterion": "works", "status": "pass"}]}, **extra}


def event(s, ws, t, kind, actor="factory", at=MID, **data):
    e = Event(workspace_id=ws, task_id=t.id, kind=kind, actor=actor, created_at=at, data=data)
    s.add(e)
    s.flush()
    return e


def measure(ws):
    with session_scope() as s:
        return workspace_metrics(s, ws)


@pytest.mark.parametrize("action,expected", [
    ("auto", 1), ("policy_plan", 1), ("human_plan", 1), ("approval", 1),
    ("approval_comment", 0), ("question_approve", 0), ("retry", 0), ("take_over", 0),
    ("message", 0), ("before", 1), ("after", 1), ("scheduler", 1), ("cancel", 0),
])
def test_autonomy_production_approvals_and_task_only_links(client, signed_in, action, expected):
    ws, user = signed_in["workspaces"][0]["id"], signed_in["user"]["id"]
    with session_scope() as s:
        o, p, t = seed(s, ws)
        event(s, ws, t, "ci_failed")  # automated repair is not assistance
        if action in {"auto", "policy_plan"}:
            s.add(Event(workspace_id=ws, outcome_id=o.id, kind="plan_approved", actor="policy",
                        created_at=MID, data={"plan_id": p.id}))
        elif action == "scheduler":
            event(s, ws, t, "retry", actor="scheduler")
        elif action in {"human_plan", "approval", "approval_comment", "question_approve"}:
            d = Decision(workspace_id=ws, task_id=t.id, title="Approve", kind=(
                "approve_plan" if action == "human_plan" else "question" if action == "question_approve" else "approve_merge"),
                status="resolved", resolution="approve", comment="Use UTF-8" if action == "approval_comment" else "",
                resolved_by=user, resolved_at=MID)
            s.add(d)
            s.flush()
            if action == "human_plan":
                s.add(Event(workspace_id=ws, outcome_id=o.id, kind="plan_approved", actor=user,
                            created_at=MID, data={"plan_id": p.id}))
            event(s, ws, t, "decision_resolved", actor=user, decision_id=d.id)
        elif action in {"message", "before", "after"}:
            at = START - timedelta(seconds=1) if action == "before" else END + timedelta(seconds=1) if action == "after" else MID
            s.add(Message(workspace_id=ws, task_id=t.id, author_id=user, author_kind="user", body="guidance", created_at=at))
        else:
            event(s, ws, t, action, actor=user)
    m = measure(ws)["autonomy"]
    assert (m["value"], m["numerator"], m["denominator"]) == (expected, expected, 1)


@pytest.mark.parametrize("attempts,check_failure,expected", [
    ([passed()], False, (1, 1, 0, 0)),
    ([{"n": 1, "result": "checks_failed"}, passed(2)], False, (0, 1, 0, 0)),
    ([{"n": 1, "result": "verification_failed"}, passed()], False, (0, 1, 0, 0)),
    ([{"n": 1, "result": "verified"}], True, (0, 1, 0, 0)),
    ([passed(checks=[])], False, (0, 0, 1, 0)),
    ([passed(verdict={"verdict": "pass", "criteria": []})], False, (0, 0, 1, 0)),
    ([{"n": 1, "result": "verified"}], False, (0, 0, 1, 0)),
    ([{"n": 1, "result": "builder_blocked"}], False, (0, 0, 0, 1)),
    ([], False, (0, 0, 0, 1)),
])
def test_first_attempt_uses_conclusive_history_not_counter(client, signed_in, attempts, check_failure, expected):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        _, _, t = seed(s, ws, evidence={"attempts": attempts}, attempt=0)
        if not attempts or attempts[0].get("result") == "builder_blocked":
            t.status = "needs_input"
        if check_failure:
            event(s, ws, t, "check_result", exit_code=1)
    m = measure(ws)["first_attempt_pass_rate"]
    assert tuple(m[k] for k in ("numerator", "denominator", "unknown_count", "excluded_count")) == expected


@pytest.mark.parametrize("failure", ["ci_failed", "smoke_result", "reverted", "ci_status", "evidence"])
def test_repaired_false_accepts_are_unique_and_durable(client, signed_in, failure):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        _, _, t = seed(s, ws, pr_number=1, evidence={"attempts": [passed()], "ci": {"state": "success"},
                                                   "smoke": {"state": "passed"}})
        event(s, ws, t, "published")
        if failure == "evidence":
            t.evidence = {**t.evidence, "failure_facts": [{"kind": "ci", "state": "failure", "at": END.isoformat()}]}
        else:
            for _ in range(2):
                event(s, ws, t, failure, exit_code=1, state="failure")
        event(s, ws, t, "ci_status", state="success")
    m = measure(ws)["verifier_false_accept_rate"]
    assert (m["value"], m["numerator"], m["denominator"]) == (1, 1, 1)


def test_cost_size_boundaries_final_artifact_and_even_medians(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        for i, (lines, cost) in enumerate([(100, 2), (99, 4), (101, 6), (400, 8), (401, 10)]):
            _, _, t = seed(s, ws)
            a = Artifact(workspace_id=ws, task_id=t.id, kind="diff", name="verified", data={"additions": lines - 1, "deletions": 1})
            s.add(a)
            s.flush()
            t.evidence = {"attempts": [passed(diff_artifact_id=a.id)]}
            # A later rejected diff must never determine delivered size.
            s.add(Artifact(workspace_id=ws, task_id=t.id, kind="diff", name="rejected", data={"additions": 999, "deletions": 0}))
            for role, value in [("builder", cost / 4), ("builder", cost / 4), ("verifier", cost / 2), ("judge", 100), ("planner", 100)]:
                s.add(Run(workspace_id=ws, outcome_id=t.outcome_id, task_id=t.id, role=role, harness="native", cost_usd=value))
        seed(s, ws)  # missing legacy size and cost
    m = measure(ws)["median_delivered_task_cost"]
    assert m["value"] == {"small": {"value": 3, "sample_count": 2}, "medium": {"value": 7, "sample_count": 2},
                          "large": {"value": 10, "sample_count": 1}}
    assert (m["sample_count"], m["unknown_count"], m["unknown_size_count"], m["unknown_cost_count"]) == (5, 1, 1, 1)


def test_failure_evidence_survives_overwrites(client, signed_in):
    from aifactory.pipeline.delivery import TaskRunner

    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        _, _, t = seed(s, ws)
        task_id = t.id
    runner = TaskRunner(task_id, None, None)
    for kind in ("ci", "smoke"):
        runner.evidence(kind, {"state": "failure"})
        runner.evidence(kind, {"state": "success"})
    with session_scope() as s:
        ev = s.get(Task, task_id).evidence
        assert ev["ci"]["state"] == ev["smoke"]["state"] == "success"
        assert [f["kind"] for f in ev["failure_facts"]] == ["ci", "smoke"]


def test_stats_empty_nulls_legacy_compatibility_and_tenancy(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    r = client.get(f"/api/workspaces/{ws}/stats")
    assert r.status_code == 200
    data = r.json()
    assert {"outcomes", "tasks", "cost", "tokens", "human", "harnesses"} <= data.keys()
    assert len(data["metrics"]) == 4
    for name, m in data["metrics"].items():
        assert isinstance(m["definition"], dict)
        assert m["unknown_count"] == m["excluded_count"] == 0
        if name != "median_delivered_task_cost":
            assert m["value"] is None and m["denominator"] == 0
        else:
            assert all(v["value"] is None for v in m["value"].values())
    other = TestClient(client.app)
    assert other.get(f"/api/workspaces/{ws}/stats").status_code == 401
    other.post("/auth/dev", json={"email": "other@test.com", "organization": "Other"})
    other_ws = other.get("/api/me").json()["workspaces"][0]["id"]
    assert other.get(f"/api/workspaces/{ws}/stats").status_code in (403, 404)
    with session_scope() as s:
        seed(s, other_ws, evidence={"attempts": [passed()]})
    assert measure(ws)["first_attempt_pass_rate"]["denominator"] == 0


def test_benchmark_source_exclusions(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        o, _, _ = seed(s, ws, evidence={"attempts": [passed()]})
        o.source = "benchmark"
        seed(s, ws, evidence={"source": "benchmark", "attempts": [passed()]})
    m = measure(ws)
    assert m["autonomy"]["denominator"] == 1
    assert m["first_attempt_pass_rate"]["denominator"] == 0
    assert m["first_attempt_pass_rate"]["excluded_count"] == 2


def test_unknown_cost_excludes_entire_task_and_benchmark_runs(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        for known in (True, False):
            _, _, t = seed(s, ws)
            a = Artifact(workspace_id=ws, task_id=t.id, kind="diff", name="final", data={"additions": 50, "deletions": 50})
            s.add(a)
            s.flush()
            t.evidence = {"attempts": [passed(diff_artifact_id=a.id)]}
            s.add_all([
                Run(workspace_id=ws, outcome_id=t.outcome_id, task_id=t.id, role="builder", harness="native", cost_usd=2),
                Run(workspace_id=ws, outcome_id=t.outcome_id, task_id=t.id, role="verifier", harness="native",
                    cost_usd=4, cost_known=known),
                Run(workspace_id=ws, outcome_id=t.outcome_id, task_id=t.id, role="builder", harness="native",
                    cost_usd=999, result={"source": "benchmark"}),
            ])
    m = measure(ws)["median_delivered_task_cost"]
    assert m["value"]["small"] == {"value": 6, "sample_count": 1}
    assert m["unknown_count"] == m["unknown_cost_count"] == 1
    assert m["unknown_size_count"] == 0
    data = client.get(f"/api/workspaces/{ws}/stats").json()
    assert data["cost"]["usd"] == 12
    assert data["tasks"]["first_attempt"] == 0  # neither task has merged
    assert data["metrics"]["first_attempt_pass_rate"]["numerator"] == 2


def test_legacy_final_verified_diff_run_link(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        _, _, t = seed(s, ws, evidence={"attempts": [{"n": 1, "result": "verification_failed"}, passed(2)]})
        for attempt, lines in [(1, 700), (2, 100), (3, 900)]:
            r = Run(workspace_id=ws, outcome_id=t.outcome_id, task_id=t.id, role="builder", harness="native",
                    attempt=attempt, cost_usd=1)
            s.add(r)
            s.flush()
            s.add(Artifact(workspace_id=ws, task_id=t.id, run_id=r.id, kind="diff", name="diff",
                           data={"additions": lines, "deletions": 0}))
    m = measure(ws)["median_delivered_task_cost"]
    assert m["value"]["small"] == {"value": 3, "sample_count": 1}
    assert m["value"]["large"]["value"] is None


@pytest.mark.parametrize("guidance,expected", [(False, 1), (True, 0)])
def test_autonomy_real_admission_event(client, signed_in, guidance, expected):
    from aifactory.models import User
    from aifactory.signals import outcome_from_text

    ws, user = signed_in["workspaces"][0]["id"], signed_in["user"]["id"]
    with session_scope() as s:
        o = outcome_from_text(s, ws, s.get(User, user), "Deliver", "Requested work", [])
        admission = s.query(Event).filter_by(outcome_id=o.id, kind="outcome_created").one()
        assert admission.actor == user and admission.created_at >= o.created_at
        o.status = "completed"
        o.completed_at = admission.created_at + timedelta(hours=1)
        if guidance:
            s.add(Message(workspace_id=ws, outcome_id=o.id, author_kind="user", author_id=user,
                          body="Please change the approach", created_at=admission.created_at + timedelta(seconds=1)))
    m = measure(ws)["autonomy"]
    assert (m["numerator"], m["denominator"], m["value"]) == (expected, 1, expected)


@pytest.mark.parametrize("kind", ["failure_facts", "ci", "smoke", "revert"])
@pytest.mark.parametrize("when,expected,unknown", [(START, 0, 0), (MID, 0, 0), (END, 1, 0), (None, 0, 1)])
@pytest.mark.parametrize("publication_event", [False, True])
def test_false_accept_evidence_requires_subsequent_failure(client, signed_in, kind, when, expected, unknown, publication_event):
    ws = signed_in["workspaces"][0]["id"]
    fact = {"state": "failure", **({"at": when.isoformat()} if when else {})}
    with session_scope() as s:
        ev = {"attempts": [passed()], "published": {"at": MID.isoformat()},
              kind: [fact] if kind == "failure_facts" else fact}
        _, _, t = seed(s, ws, pr_number=1, evidence=ev)
        if publication_event:
            event(s, ws, t, "published", at=MID)
    m = measure(ws)["verifier_false_accept_rate"]
    assert (m["numerator"], m["denominator"], m["value"], m["unknown_count"]) == (expected, 1 - unknown, None if unknown else expected, unknown)


@pytest.mark.parametrize("kind", ["ci_failed", "smoke_result", "reverted"])
@pytest.mark.parametrize("after", [False, True])
def test_false_accept_events_require_subsequent_failure(client, signed_in, kind, after):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        _, _, t = seed(s, ws, pr_number=1, evidence={"attempts": [passed()]})
        if after:
            event(s, ws, t, "published")
        event(s, ws, t, kind, exit_code=1)
        if not after:
            event(s, ws, t, "published")
    m = measure(ws)["verifier_false_accept_rate"]
    assert (m["numerator"], m["denominator"], m["unknown_count"]) == (int(after), 1, 0)


@pytest.mark.parametrize("kind", ["failure_facts", "ci", "smoke", "revert"])
def test_republication_does_not_erase_evidence_only_false_accept(client, signed_in, kind):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        fact = {"state": "failure", "at": MID.isoformat()}
        ev = {"attempts": [passed()], "published": {"at": END.isoformat()},
              "ci": {"state": "success"}, "smoke": {"state": "passed"},
              kind: [fact, fact] if kind == "failure_facts" else fact}
        _, _, t = seed(s, ws, pr_number=1, evidence=ev)
        event(s, ws, t, "published", at=START)
        # No failure event: evidence may be committed before event emission.
        event(s, ws, t, "published", at=END)
    m = measure(ws)["verifier_false_accept_rate"]
    assert (m["numerator"], m["denominator"], m["value"], m["unknown_count"]) == (1, 1, 1, 0)


def test_legacy_first_attempt_counter_only_counts_merged_durable_passes(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        seed(s, ws, merge_sha="merged-pass", evidence={"attempts": [passed()]})
        _, _, waiting = seed(s, ws, evidence={"attempts": [passed()]})
        waiting.status = "awaiting_approval"
        seed(s, ws, merge_sha="merged-repair", attempt=1,
             evidence={"attempts": [{"n": 1, "result": "checks_failed"}, passed()]})
        _, _, failed_check = seed(s, ws, merge_sha="merged-check", evidence={"attempts": [passed()]})
        event(s, ws, failed_check, "check_result", exit_code=1)
        seed(s, ws, merge_sha="merged-unknown", evidence={"attempts": [{"n": 1, "result": "verified"}]})
    response = client.get(f"/api/workspaces/{ws}/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["tasks"]["merged"] == 4
    assert data["tasks"]["first_attempt"] == 1
    assert data["metrics"]["first_attempt_pass_rate"]["numerator"] == 2
    assert data["metrics"]["first_attempt_pass_rate"]["denominator"] == 4
    assert data["metrics"]["first_attempt_pass_rate"]["unknown_count"] == 1


@pytest.mark.parametrize("source", ["event", "evidence", "both"])
@pytest.mark.parametrize("at,expected", [
    (START - timedelta(seconds=1), 1),
    (START, 0), (MID, 0), (END, 0),
    (END + timedelta(seconds=1), 1),
])
def test_external_merge_is_assistance_even_with_factory_actor(client, signed_in, source, at, expected):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        _, _, t = seed(s, ws, merge_sha="external-merge")
        if source in {"evidence", "both"}:
            # TaskRunner.merge() records this fact and emits a factory event.
            t.evidence = {"merge": {"outside": True, "by": "github-user", "at": at.isoformat() + "+00:00"}}
            event(s, ws, t, "merged", at=at)
        if source in {"event", "both"}:
            # reconcile._handle_merged() uses the default factory actor.
            event(s, ws, t, "merged_outside", at=at)
    m = measure(ws)["autonomy"]
    assert (m["numerator"], m["denominator"], m["unknown_count"], m["value"]) == (expected, 1, 0, expected)
    response = client.get(f"/api/workspaces/{ws}/stats")
    assert response.status_code == 200
    assert response.json()["metrics"]["autonomy"] == m


def test_factory_merge_is_not_human_assistance(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        _, _, t = seed(s, ws, merge_sha="automatic-merge", evidence={"merge": {"at": MID.isoformat()}})
        event(s, ws, t, "merged")
    m = measure(ws)["autonomy"]
    assert (m["numerator"], m["denominator"], m["value"]) == (1, 1, 1)


def test_undated_external_merge_is_unknown_not_autonomous(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        seed(s, ws, evidence={"merge": {"outside": True}})
    m = measure(ws)["autonomy"]
    assert (m["numerator"], m["denominator"], m["unknown_count"], m["value"]) == (0, 0, 1, None)


@pytest.mark.parametrize("known_failure", [False, True])
def test_unordered_false_accepts_do_not_dilute_known_population(client, signed_in, known_failure):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        # A retained failure with no date cannot be ordered even when the
        # publication date is known. It must not become a negative sample.
        seed(s, ws, pr_number=1, evidence={"attempts": [passed()],
             "published": {"at": START.isoformat()}, "failure_facts": [{"kind": "ci", "state": "failure"}]})
        _, _, t = seed(s, ws, pr_number=2, evidence={"attempts": [passed()],
                       "published": {"at": START.isoformat()}})
        if known_failure:
            event(s, ws, t, "ci_failed", at=MID)
    m = measure(ws)["verifier_false_accept_rate"]
    assert (m["numerator"], m["denominator"], m["value"], m["unknown_count"]) == (int(known_failure), 1, int(known_failure), 1)
    response = client.get(f"/api/workspaces/{ws}/stats")
    assert response.status_code == 200
    assert response.json()["metrics"]["verifier_false_accept_rate"] == m


@pytest.mark.parametrize("role", ["builder", "verifier"])
def test_benchmark_trial_link_excluded_from_delivery_and_legacy_stats(client, signed_in, role):
    from aifactory.metrics import benchmark
    from aifactory.models import BenchmarkSuite, BenchmarkTrial

    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        _, _, t = seed(s, ws)
        diff = Artifact(workspace_id=ws, task_id=t.id, kind="diff", name="final",
                        data={"additions": 100, "deletions": 0})
        suite = BenchmarkSuite(workspace_id=ws, name="delivery", revision=1, schema_version=1,
                               content_hash="a" * 64, tasks=[])
        s.add_all([diff, suite])
        s.flush()
        t.evidence = {"attempts": [passed(diff_artifact_id=diff.id)]}
        trial = BenchmarkTrial(workspace_id=ws, suite_id=suite.id, suite_revision=1,
                               task_key="T1", trial_number=1, outcome_id=t.outcome_id, task_id=t.id)
        s.add(trial)
        s.flush()
        production = Run(workspace_id=ws, outcome_id=t.outcome_id, task_id=t.id, role="builder",
                         harness="native", cost_usd=2, input_tokens=3, output_tokens=4, status="succeeded")
        # No JSON source marker: the existing dedicated FK is sufficient.
        benchmark_run = Run(workspace_id=ws, outcome_id=t.outcome_id, task_id=t.id, role=role,
                            harness="codex", cost_usd=999, input_tokens=999, output_tokens=999,
                            benchmark_trial_id=trial.id, status="succeeded")
        s.add_all([production, benchmark_run])
        s.flush()
        assert benchmark(benchmark_run) is True
        assert benchmark(production) is False
    response = client.get(f"/api/workspaces/{ws}/stats")
    assert response.status_code == 200
    data = response.json()
    cost = data["metrics"]["median_delivered_task_cost"]
    assert cost["value"]["small"] == {"value": 2, "sample_count": 1}
    assert (cost["sample_count"], cost["unknown_count"]) == (1, 0)
    assert data["cost"] == {"usd": 2, "known": True, "per_completed_task": 2}
    assert data["tokens"] == 7
    assert data["harnesses"] == {"native": {"runs": 1, "succeeded": 1},
                                 "codex": {"runs": 0, "succeeded": 0},
                                 "anton": {"runs": 0, "succeeded": 0}}


def test_autonomy_many_outcomes_keeps_histories_local(client, signed_in):
    ws, user = signed_in["workspaces"][0]["id"], signed_in["user"]["id"]
    cases = ("automatic", "approval", "outside_window", "message", "retry", "decision", "external", "unknown")
    with session_scope() as s:
        for i in range(120):
            case = cases[i % len(cases)]
            o, p, t = seed(s, ws)
            for _ in range(20):
                event(s, ws, t, "retry", actor="scheduler")
            if case == "approval":
                d = Decision(workspace_id=ws, outcome_id=o.id, task_id=t.id, kind="approve_plan",
                             title="Approve", status="resolved", resolution="approve", resolved_by=user,
                             resolved_at=MID)
                s.add(d)
                s.flush()
                # Both direct and task links must be indexed without duplication.
                e = event(s, ws, t, "decision_resolved", actor=user, decision_id=d.id)
                e.outcome_id = o.id
                s.add(Event(workspace_id=ws, outcome_id=o.id, kind="plan_approved", actor=user,
                            created_at=MID, data={"plan_id": p.id}))
            elif case in {"outside_window", "message"}:
                for at in ([START - timedelta(seconds=1), END + timedelta(seconds=1)]
                           if case == "outside_window" else [MID]):
                    s.add(Message(workspace_id=ws, task_id=t.id, author_kind="user", author_id=user,
                                  body="Guidance", created_at=at))
            elif case == "retry":
                event(s, ws, t, "retry", actor=user)
            elif case == "decision":
                s.add(Decision(workspace_id=ws, task_id=t.id, kind="question", title="Guidance",
                               status="resolved", resolution="approve", resolved_by=user, resolved_at=MID))
            elif case == "external":
                t.evidence = {"merge": {"outside": True, "at": MID.isoformat()}}
                event(s, ws, t, "merged_outside")
            elif case == "unknown":
                o.completed_at = None
    m = measure(ws)["autonomy"]
    # Each group of eight has three autonomous, four assisted and one unknown.
    assert (m["numerator"], m["denominator"], m["unknown_count"], m["value"]) == (45, 105, 15, 3 / 7)
    response = client.get(f"/api/workspaces/{ws}/stats")
    assert response.status_code == 200
    assert response.json()["metrics"]["autonomy"] == m
