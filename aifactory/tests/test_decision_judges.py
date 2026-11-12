"""Judge persistence/input/policy tests: no provider or network required."""
from datetime import timedelta

import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError

from aifactory import db, serializers
from aifactory.decision_judges import build_context, eligible, question_fingerprint, validate_report
from aifactory.inference import workspace_models
from aifactory.models import Decision, DecisionReview, Message, Outcome, Plan, Repository, Task, Workspace


def metadata(role="builder", **flags):
    return {"origin": {"role": role}, "safety": {
        "approval": False, "budget": False, "takeover": False, "protected_paths": False,
        "paths": [], **flags}}


@pytest.fixture
def data(signed_in):
    ws = signed_in["workspaces"][0]["id"]
    uid = signed_in["user"]["id"]
    now = db.utcnow()
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="team/app", config={"context": "Team notes", "protected_paths": ["deploy/**"]})
        outcome = Outcome(workspace_id=ws, number=42, title="Ship", brief="Brief", acceptance=["Outcome acceptance"])
        s.add_all([repo, outcome])
        s.flush()
        outcome.repo_ids = [repo.id]
        plan = Plan(outcome_id=outcome.id)
        s.add(plan)
        s.flush()
        task = Task(workspace_id=ws, outcome_id=outcome.id, plan_id=plan.id, repo_id=repo.id,
                    key="T1", title="Task", spec="Specification", acceptance=["Task acceptance"])
        s.add(task)
        s.flush()
        prior = Decision(workspace_id=ws, outcome_id=outcome.id, task_id=task.id, kind="question",
                         title="Earlier?", options=[{"value": "answer"}], status="resolved", resolution="answer",
                         comment="Use the durable queue.", resolved_by=uid, resolved_at=now - timedelta(days=1),
                         created_at=now - timedelta(days=2), reusable=False, scope="item")
        current = Decision(workspace_id=ws, outcome_id=outcome.id, task_id=task.id, kind="question", title="Which queue?",
                           options=[{"value": "answer", "label": "Answer"}], metadata_json=metadata(), created_at=now)
        s.add_all([prior, current])
        s.flush()
        return ws, uid, repo.id, outcome.id, task.id, prior.id, current.id


def test_additive_table_and_legacy_serialization(data):
    ws, _, _, _, _, prior, current = data
    engine = db.init_engine()
    # Simulate an existing deployment without the new table; init_engine adds it.
    DecisionReview.__table__.drop(engine)
    engine = db.init_engine()
    assert "decision_reviews" in inspect(engine).get_table_names()
    with db.session_scope() as s:
        assert serializers.decision(s.get(Decision, prior))["judge"] is None
        review = DecisionReview(workspace_id=ws, decision_id=current, report={"verdict": "hold"})
        s.add(review)
        s.flush()
        s.expire(s.get(Decision, current), ["review"])
        view = serializers.decision(s.get(Decision, current))["judge"]
        assert view["mode"] == "decide" and view["report"] == {"verdict": "hold"}
        assert view["created_at"] and view["question_revision"] == 1
    with pytest.raises(IntegrityError), db.session_scope() as s:
        s.add(DecisionReview(workspace_id=ws, decision_id=current))
        s.flush()


def test_settings_round_trip_and_model_fallback(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    assert signed_in["workspaces"][0]["settings"]["decision_judges"] == "decide"
    path = f"/api/workspaces/{ws}"
    for mode in ("off", "suggest", "decide"):
        response = client.patch(path, json={"settings": {"decision_judges": mode, "models": {"default": "base", "judge": "judge-model"}}})
        assert response.status_code == 200, response.text
        assert response.json()["settings"]["decision_judges"] == mode
        assert client.get("/api/me").json()["workspaces"][0]["settings"]["decision_judges"] == mode
    for mode in ("auto", "", 1, [], {}, None):
        response = client.patch(path, json={"settings": {"decision_judges": mode}})
        assert response.status_code == 422, response.text
    assert workspace_models({}).judge == workspace_models({}).planner
    assert workspace_models({"models": {"default": "base"}}).judge == "base"
    assert workspace_models({"models": {"default": "base", "judge": "custom"}}).judge == "custom"


def test_context_complete_attributed_and_scoped(data):
    ws, uid, repo, outcome, task, prior, current = data
    with db.session_scope() as s:
        s.add(Workspace(id="foreign", org_id=s.get(Workspace, ws).org_id, name="Foreign", slug="foreign"))
        s.flush()
        for i in range(305):
            s.add(Decision(workspace_id=ws, kind="question", title=f"Recorded {i}", status="resolved",
                           resolution="answer" if i % 2 else "", comment="Recorded text", resolved_by=uid,
                           scope=("item", "repository", "workspace")[i % 3], outcome_id=outcome,
                           task_id=task if i % 2 else None))
        s.add_all([
            Decision(workspace_id=ws, kind="question", title="Empty", status="resolved"),
            Decision(workspace_id="foreign", kind="question", title="Foreign secret", status="resolved", comment="SECRET"),
            Repository(id="foreign-repo", workspace_id="foreign", full_name="foreign/repo", config={"context": "SECRET"}),
            Outcome(id="foreign-outcome", workspace_id="foreign", number=1, title="SECRET", brief="SECRET"),
            Message(workspace_id=ws, outcome_id=outcome, author_id=uid, body="Outcome guidance"),
            Message(workspace_id=ws, outcome_id=outcome, task_id=task, author_id=uid, body="Task guidance"),
            Message(workspace_id=ws, outcome_id=outcome, task_id="other-task", body="Other task secret"),
            Message(workspace_id="foreign", outcome_id=outcome, body="SECRET"),
        ])
        s.flush()
        ctx = build_context(s, s.get(Decision, current))
        assert len(ctx["records"]) == 306  # neither reusable-only, 12, nor 300
        record = next(r for r in ctx["records"] if r["id"] == prior)
        assert record["author"]["id"] == uid and record["author"]["name"] == "Ian"
        assert record["created_at"] and record["resolved_at"] and record["scope"] == "item"
        assert record["outcome"]["id"] == outcome and record["task_id"] == task
        assert record["repositories"] == [{"id": repo, "full_name": "team/app"}]
        assert ctx["outcome"]["brief"] == "Brief" and ctx["outcome"]["acceptance"] == ["Outcome acceptance"]
        assert ctx["task"]["spec"] == "Specification" and ctx["task"]["acceptance"] == ["Task acceptance"]
        assert ctx["repositories"][0]["team_notes"] == "Team notes"
        assert [m["body"] for m in ctx["guidance"]] == ["Outcome guidance", "Task guidance"]
        assert all(m["author"]["id"] == uid for m in ctx["guidance"])
        assert "SECRET" not in str(ctx)
        # Even poisoned associations cannot pull another workspace's brief/notes in.
        q = s.get(Decision, current)
        q.task_id, q.outcome_id = None, "foreign-outcome"
        assert build_context(s, q)["outcome"] is None
        q.outcome_id = outcome
        s.get(Outcome, outcome).repo_ids = ["foreign-repo"]
        assert build_context(s, q)["repositories"] == []


@pytest.mark.parametrize("kind,role,flags,options", [
    ("approve_plan", "planner", {}, ["approve"]),
    ("approve_merge", "builder", {}, ["approve"]),
    ("approve_release", "builder", {}, ["approve"]),
    ("budget", "builder", {}, ["retry"]),
    ("takeover", "builder", {}, ["answer"]),
    ("question", "builder", {"approval": True}, ["answer"]),
    ("question", "builder", {"budget": True}, ["answer"]),
    ("question", "planner", {"takeover": True}, ["answer"]),
    ("unblock", "builder", {}, ["retry", "take_over"]),
    ("question", "builder", {}, ["answer", "approve"]),
    ("question", "builder", {"protected_paths": True}, ["answer"]),
    *[("question", "builder", {"paths": [p]}, ["answer"]) for p in
      (".env", ".env.prod", ".github/workflows/ci.yml", "certs/key.pem", "cert.key", "deploy/prod.yml", "../secret", "src/../.env")],
])
def test_governance_exclusions(data, kind, role, flags, options):
    with db.session_scope() as s:
        q = s.get(Decision, data[-1])
        q.kind, q.metadata_json, q.options = kind, metadata(role, **flags), [{"value": v} for v in options]
        assert not eligible(q, [s.get(Repository, data[2])])


@pytest.mark.parametrize("meta", [None, {}, {"origin": "builder"}, metadata(approval=None),
                                  {"origin": {"role": "builder"}, "safety": {}}, metadata(paths=".env")])
def test_missing_or_malformed_safety_is_ineligible(data, meta):
    with db.session_scope() as s:
        q = s.get(Decision, data[-1])
        q.metadata_json = meta
        assert not eligible(q)


def test_a_decision_marked_for_people_is_never_judged(data):
    with db.session_scope() as s:
        q = s.get(Decision, data[-1])
        assert eligible(q)
        q.metadata_json = {**metadata(), "people_only": "A person decides this"}
        assert not eligible(q)


def report(prior):
    return {"verdict": "decide", "answer": "Use the durable queue.", "resolution": "answer",
            "summary": "The team answered this.", "citations": [{"decision_id": prior, "passage": "durable queue"}],
            "closest_decisions": [prior]}


def test_valid_report_planner_cannot_approve_and_fingerprint(data):
    with db.session_scope() as s:
        q = s.get(Decision, data[-1])
        assert eligible(q)
        fingerprint = question_fingerprint(q)
        ctx = build_context(s, q)
        assert validate_report(s, q, report(data[-2]), ctx)["verdict"] == "decide"
        s.get(Decision, data[-2]).scope = "workspace"
        q.task_id, q.metadata_json = None, metadata("planner")
        q.options = [{"value": "approve"}, {"value": "answer"}, {"value": "cancel"}]
        assert eligible(q)
        assert question_fingerprint(q) != fingerprint
        ctx = build_context(s, q)
        assert validate_report(s, q, report(data[-2]), ctx)["resolution"] == "answer"
        invalid = {**report(data[-2]), "resolution": "approve"}
        with pytest.raises(ValueError):
            validate_report(s, q, invalid, ctx)


@pytest.mark.parametrize("citations", [None, {}, [], ["id"], [{}], [{"id": "x", "quote": "text"}],
    [{"decision_id": [], "passage": "x"}], [{"decision_id": "fabricated", "passage": "text"}],
    [{"decision_id": "PRIOR", "passage": ""}], [{"decision_id": "PRIOR", "passage": "  "}],
    [{"decision_id": "PRIOR", "passage": "Durable queue"}],
    [{"decision_id": "PRIOR", "passage": "Earlier?"}],
    [{"decision_id": "PRIOR", "passage": 3}],
])
def test_invalid_citation_forms(data, citations):
    with db.session_scope() as s:
        q = s.get(Decision, data[-1])
        if isinstance(citations, list):
            citations = [{**c, "decision_id": data[-2]} if isinstance(c, dict) and c.get("decision_id") == "PRIOR" else c for c in citations]
        with pytest.raises(ValueError):
            validate_report(s, q, {**report(data[-2]), "citations": citations}, build_context(s, q))


@pytest.mark.parametrize("invalid", ["not_supplied", "foreign", "later_created", "later_resolved", "unresolved", "self", "snapshot_passage", "corrected"])
def test_citations_are_earlier_supplied_local_and_uncorrected(data, invalid):
    with db.session_scope() as s:
        q, prior = s.get(Decision, data[-1]), s.get(Decision, data[-2])
        ctx = build_context(s, q)
        candidate = report(prior.id)
        if invalid == "not_supplied":
            ctx["records"] = []
        elif invalid == "foreign":
            prior.workspace_id = "foreign"
        elif invalid == "later_created":
            prior.created_at = q.created_at
        elif invalid == "later_resolved":
            prior.resolved_at = q.created_at
        elif invalid == "unresolved":
            prior.status = "open"
        elif invalid == "self":
            candidate["citations"][0]["decision_id"] = q.id
        elif invalid == "snapshot_passage":
            ctx["records"][0]["comment"] = "Different answer"
        else:
            correction = Decision(workspace_id=data[0], kind="question", title="Correction", status="resolved",
                                  resolution="answer", comment="Use a database.", resolved_by=data[1], scope="workspace",
                                  created_at=q.created_at - timedelta(hours=3), resolved_at=q.created_at - timedelta(hours=2))
            s.add(correction)
            s.flush()
            s.add(DecisionReview(workspace_id=data[0], decision_id=prior.id, correction_decision_id=correction.id))
            s.flush()
            fresh = build_context(s, q)
            assert next(r for r in fresh["records"] if r["id"] == prior.id)["superseded_by"] == correction.id
            corrected = report(correction.id)
            corrected["citations"][0]["passage"] = "Use a database."
            assert validate_report(s, q, corrected, fresh)["verdict"] == "decide"
        with pytest.raises(ValueError):
            validate_report(s, q, candidate, ctx)


@pytest.mark.parametrize("verdict", ["hold", "send_back"])
def test_non_decide_reports_do_not_require_citations(data, verdict):
    with db.session_scope() as s:
        q = s.get(Decision, data[-1])
        candidate = {"verdict": verdict, "answer": "Read the task specification.", "summary": "Not covered by prior decisions."}
        assert validate_report(s, q, candidate, build_context(s, q))["citations"] == []


def test_validator_rejects_stale_input_and_governance(data):
    with db.session_scope() as s:
        q = s.get(Decision, data[-1])
        ctx = build_context(s, q)
        q.body = "A revised question"
        with pytest.raises(ValueError, match="changed"):
            validate_report(s, q, report(data[-2]), ctx)
        q.metadata_json = metadata(budget=True)
        with pytest.raises(ValueError, match="governance"):
            validate_report(s, q, report(data[-2]), build_context(s, q))


def test_open_decision_accepts_and_invalidates_safety_metadata(data):
    from aifactory.pipeline.runtime import open_decision

    ws, _, _, outcome, task, _, current = data
    assert open_decision(ws, "question", "Updated", "Body", outcome_id=outcome, task_id=task,
                         metadata=metadata()) == current
    with db.session_scope() as s:
        assert eligible(s.get(Decision, current))
    # Updating without provenance must not inherit an old safe classification.
    open_decision(ws, "question", "Different", "Different body", outcome_id=outcome, task_id=task)
    with db.session_scope() as s:
        assert not eligible(s.get(Decision, current))


def test_legacy_schema_adds_nullable_metadata():
    from sqlalchemy import text

    engine = db.init_engine()
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE decisions DROP COLUMN metadata_json"))
    engine = db.init_engine()
    assert "metadata_json" in {c["name"] for c in inspect(engine).get_columns("decisions")}


@pytest.mark.parametrize("status", ["resolved", "dismissed", "superseded"])
@pytest.mark.parametrize("verdict", ["decide", "send_back", "hold"])
def test_late_report_rejected_after_human_closes_decision(data, status, verdict):
    ws, uid, _, _, _, prior, current = data
    with db.session_scope() as reviewing:
        q = reviewing.get(Decision, current)
        context = build_context(reviewing, q)
        # A model runs outside the transaction. Its session still retains the
        # original identity-mapped question when the result arrives.
        reviewing.commit()
        with db.session_scope() as human:
            decision = human.get(Decision, current)
            decision.status = status
            decision.resolution = "answer" if status == "resolved" else "dismiss"
            decision.comment = "The person's answer takes precedence."
            decision.resolved_by = uid
            decision.resolved_at = db.utcnow()
            resolved_at = decision.resolved_at
        assert q.status == "open"  # checking only the cached object is unsafe
        with pytest.raises(ValueError, match="no longer open"):
            validate_report(reviewing, q, {**report(prior), "verdict": verdict}, context)

    with db.session_scope() as checking:
        decision = checking.get(Decision, current)
        assert decision.workspace_id == ws
        assert decision.status == status
        assert decision.resolution == ("answer" if status == "resolved" else "dismiss")
        assert decision.comment == "The person's answer takes precedence."
        assert decision.resolved_by == uid
        assert decision.resolved_at == resolved_at
        assert decision.review is None


@pytest.mark.parametrize("changes", [
    {"title": "A different question"},
    {"body": "Revised question context"},
    {"options": [{"value": "retry", "label": "Try again"}]},
    {"metadata_json": metadata(budget=True)},
])
def test_late_report_rejected_after_question_revised_in_another_session(data, changes):
    with db.session_scope() as reviewing:
        q = reviewing.get(Decision, data[-1])
        context = build_context(reviewing, q)
        original_fingerprint = question_fingerprint(q)
        reviewing.commit()
        with db.session_scope() as editor:
            current = editor.get(Decision, q.id)
            for field, value in changes.items():
                setattr(current, field, value)
        # The worker's identity map still holds the original question.
        assert question_fingerprint(q) == original_fingerprint
        with pytest.raises(ValueError, match="Question changed"):
            validate_report(reviewing, q, report(data[-2]), context)
        # Validation refreshes the same instance the caller will use to apply a
        # verdict; no stale fields may survive the locked read.
        assert q.status == "open"
        for field, value in changes.items():
            assert getattr(q, field) == value

    with db.session_scope() as checking:
        current = checking.get(Decision, data[-1])
        assert current.status == "open"
        assert current.resolution == "" and current.comment == ""
        assert current.review is None
        for field, value in changes.items():
            assert getattr(current, field) == value


def automatic(data, *, verdict="decide", scope="repository", task_status="running", outcome_status="running"):
    ws, _, _, oid, tid, _, qid = data
    with db.session_scope() as s:
        q = s.get(Decision, qid)
        q.status, q.resolution, q.comment, q.scope = "resolved", "answer", "Original judge answer", scope
        q.resolved_at = db.utcnow()
        s.get(Task, tid).status = task_status
        s.get(Outcome, oid).status = outcome_status
        s.add(DecisionReview(workspace_id=ws, decision_id=qid, state="completed", mode="decide",
            effective_verdict=verdict, proposed_verdict=verdict, answer=q.comment,
            summary="Original summary", citations=[{"decision_id": data[-2], "passage": "durable queue"}],
            finished_at=db.utcnow()))


@pytest.mark.parametrize("verdict", ["decide", "send_back"])
def test_member_overrule_once_guidance_and_precedence(client, data, monkeypatch, verdict):
    from aifactory.models import Membership
    from aifactory.pipeline.runtime import precedents, take_guidance
    from aifactory.events import bus

    automatic(data, verdict=verdict)
    ws, uid, repo, oid, tid, _, qid = data
    with db.session_scope() as s:
        s.query(Membership).filter_by(workspace_id=ws, user_id=uid).one().role = "member"
    take = take_guidance(oid, tid)
    other = take_guidance(oid, "other-task")
    notifications = []
    monkeypatch.setattr(bus, "publish", lambda payload: notifications.append(payload))
    path = f"/api/workspaces/{ws}/decisions/{qid}/overrule"
    body = {"resolution": "answer", "answer": "Use a database instead.", "reason": "Queue is obsolete"}
    response = client.post(path, json=body)
    assert response.status_code == 200, response.text
    cid = response.json()["correction_decision_id"]
    assert client.post(path, json=body).status_code == 400
    assert take() == [f"Correction to decision {qid}: Use a database instead.\nReason: Queue is obsolete"]
    assert take() == [] and other() == []
    assert notifications
    with db.session_scope() as s:
        q, c = s.get(Decision, qid), s.get(Decision, cid)
        assert q.status == "superseded" and q.comment == q.review.answer == "Original judge answer"
        assert q.review.correction_decision_id == cid and q.review.summary == "Original summary"
        assert c.resolved_by == uid and c.resolved_at and c.reusable
        assert c.attribution["reason"] == body["reason"] and c.attribution["supersedes"] == qid
        assert (c.scope, c.task_id, c.outcome_id) == ("repository", tid, oid)
        assert s.get(Task, tid).status == s.get(Outcome, oid).status == "running"
        assert s.query(Message).filter_by(outcome_id=oid).count() == 1
        ctx = build_context(s, q)
        assert next(r for r in ctx["records"] if r["id"] == qid)["superseded_by"] == cid
        assert next(r for r in ctx["records"] if r["id"] == cid)["priority"] == "correction"
    with db.session_scope() as s:
        newer = s.get(Decision, data[-2])
        newer.reusable, newer.scope, newer.resolved_at = True, "workspace", db.utcnow() + timedelta(seconds=1)
    assert precedents(ws, repo, limit=1)[0].id == cid
    assert all(d.id != qid for d in precedents(ws, repo))
    assert all(d.id not in (qid, cid) for d in precedents(ws, "different-repo"))
    for url in (f"/api/workspaces/{ws}/decisions", f"/api/workspaces/{ws}/outcomes/{oid}"):
        payload = client.get(url).json()
        rows = payload if isinstance(payload, list) else payload["decisions"]
        original = next(d for d in rows if d["id"] == qid)
        assert original["resolved_by_name"] == "Decision judge"
        assert original["correction"]["resolved_by_name"] == "Ian"
        assert original["correction"]["attribution"]["reason"] == body["reason"]
        assert original["judge"]["citations"]


@pytest.mark.parametrize("verdict", ["decide", "send_back"])
@pytest.mark.parametrize("outcome_status", ["running", "attention"])
@pytest.mark.parametrize("task_status", [
    "pending", "queued", "running", "waiting_ci", "needs_input", "awaiting_approval",
    "blocked", "failed", "smoke_failed",
])
def test_active_work_receives_correction_without_resuming(client, data, verdict, task_status, outcome_status):
    from aifactory.pipeline.runtime import guidance_history, take_guidance

    automatic(data, verdict=verdict, task_status=task_status, outcome_status=outcome_status)
    ws, _, _, oid, tid, _, qid = data
    take = take_guidance(oid, tid)
    other = take_guidance(oid, "other-task")
    path = f"/api/workspaces/{ws}/decisions/{qid}/overrule"
    body = {"resolution": "answer", "answer": "Correction", "reason": "New evidence"}
    response = client.post(path, json=body)
    assert response.status_code == 200, response.text
    expected = [f"Correction to decision {qid}: Correction\nReason: New evidence"]
    assert take() == expected
    assert take() == [] and other() == []
    # A CI wait or human pause can outlive the current agent session: the next
    # session must also see the correction in its initial guidance history.
    assert guidance_history(oid, tid) == expected
    assert guidance_history(oid, "other-task") == []
    assert client.post(path, json=body).status_code == 400
    with db.session_scope() as s:
        assert s.query(Message).filter_by(outcome_id=oid, task_id=tid).count() == 1
        assert s.get(Task, tid).status == task_status
        assert s.get(Outcome, oid).status == outcome_status


@pytest.mark.parametrize("outcome_status", ["planning", "needs_input", "awaiting_approval", "blocked"])
def test_active_outcome_only_receives_correction(client, data, outcome_status):
    from aifactory.pipeline.runtime import take_guidance

    automatic(data, outcome_status=outcome_status)
    ws, _, _, oid, _, _, qid = data
    with db.session_scope() as s:
        s.get(Decision, qid).task_id = None
    take = take_guidance(oid)
    response = client.post(f"/api/workspaces/{ws}/decisions/{qid}/overrule",
        json={"resolution": "answer", "answer": "Correction", "reason": "New evidence"})
    assert response.status_code == 200, response.text
    assert take() == [f"Correction to decision {qid}: Correction\nReason: New evidence"]
    assert take() == []
    with db.session_scope() as s:
        assert s.get(Outcome, oid).status == outcome_status
        assert s.query(Message).filter_by(outcome_id=oid, task_id=None).count() == 1


@pytest.mark.parametrize("verdict", ["decide", "send_back"])
@pytest.mark.parametrize("task_status,outcome_status", [("completed", "running"), ("cancelled", "running"),
    ("abandoned", "running"), ("reverted", "running"), ("handed_over", "running"),
    ("unknown", "running"), ("running", "unknown"),
    ("running", "completed"), ("running", "cancelled"), ("running", "partially_completed")])
def test_terminal_work_never_restarted(client, data, verdict, task_status, outcome_status):
    automatic(data, verdict=verdict, task_status=task_status, outcome_status=outcome_status)
    response = client.post(f"/api/workspaces/{data[0]}/decisions/{data[-1]}/overrule",
        json={"resolution": "answer", "answer": "Correction", "reason": "New evidence"})
    assert response.status_code == 200, response.text
    with db.session_scope() as s:
        assert s.get(Task, data[4]).status == task_status
        assert s.get(Outcome, data[3]).status == outcome_status
        assert s.query(Message).count() == 0


def test_overrule_access_isolation_validation(client, data):
    from aifactory.models import Membership

    automatic(data)
    ws, uid, _, _, _, _, qid = data
    path = f"/api/workspaces/{ws}/decisions/{qid}/overrule"
    body = {"resolution": "answer", "answer": "Correction", "reason": "Evidence"}
    with db.session_scope() as s:
        s.query(Membership).filter_by(workspace_id=ws, user_id=uid).one().role = "viewer"
    assert client.post(path, json=body).status_code == 403
    with db.session_scope() as s:
        s.query(Membership).filter_by(workspace_id=ws, user_id=uid).one().role = "member"
        foreign = Workspace(org_id=s.get(Workspace, ws).org_id, name="Foreign", slug="foreign")
        s.add(foreign)
        s.flush()
        d = Decision(workspace_id=foreign.id, kind="question", title="Secret")
        s.add(d)
        s.flush()
        foreign_id = d.id
    assert client.post(path.replace(qid, foreign_id), json=body).status_code == 404
    for invalid in ({"reason": ""}, {"reason": "  "}, {"answer": "  "}, {"resolution": "approve"}):
        assert client.post(path, json={**body, **invalid}).status_code in (400, 422)
    assert client.post(path, json={"resolution": "answer", "answer": "Text"}).status_code == 422
    with db.session_scope() as s:
        assert s.get(Decision, qid).status == "resolved"
        assert s.get(Decision, qid).review.correction_decision_id is None
        assert s.query(Message).count() == 0
        assert s.query(Decision).filter_by(workspace_id=ws).count() == 2


@pytest.mark.parametrize("status", ["open", "judging"])
def test_held_library_and_pending_not_actionable(client, data, status):
    ws, _, _, oid, _, _, qid = data
    with db.session_scope() as s:
        s.get(Decision, qid).status = status
        s.add(DecisionReview(workspace_id=ws, decision_id=qid, state="pending"))
    assert qid not in [d["id"] for d in client.get(f"/api/workspaces/{ws}/inbox").json()["decisions"]]
    assert qid not in [d["id"] for d in client.get(f"/api/workspaces/{ws}/decisions").json()]
    payload = client.get(f"/api/workspaces/{ws}/outcomes/{oid}").json()
    assert not next(d for d in payload["decisions"] if d["id"] == qid)["actionable"]
    with db.session_scope() as s:
        s.get(Decision, qid).status = "open"
        r = s.get(Decision, qid).review
        r.state, r.effective_verdict, r.proposed_verdict = "completed", "hold", "decide"
        r.mode, r.summary, r.answer, r.finished_at = "suggest", "Needs confirmation", "Proposed answer", db.utcnow()
    for endpoint in ("inbox", "decisions"):
        payload = client.get(f"/api/workspaces/{ws}/{endpoint}").json()
        rows = payload["decisions"] if endpoint == "inbox" else payload
        held = next(d for d in rows if d["id"] == qid)
        assert held["actionable"] and held["judge"]["answer"] == "Proposed answer"
        assert held["judge"]["summary"] == "Needs confirmation"


def test_held_library_precedes_full_resolved_history(client, data):
    ws, uid, _, oid, _, _, qid = data
    now = db.utcnow()
    with db.session_scope() as s:
        s.add_all(Decision(workspace_id=ws, outcome_id=oid, kind="question",
            title=f"Resolved question {i}", status="resolved", resolution="answer",
            comment="Earlier answer", resolved_by=uid, resolved_at=now)
            for i in range(300))
        s.add(DecisionReview(workspace_id=ws, decision_id=qid, state="completed",
            effective_verdict="hold", proposed_verdict="hold", summary="Needs a person",
            finished_at=now))
        pending = Decision(workspace_id=ws, outcome_id=oid, kind="question", title="Still judging")
        s.add(pending)
        s.flush()
        pending_id = pending.id
        s.add(DecisionReview(workspace_id=ws, decision_id=pending_id, state="pending"))
    response = client.get(f"/api/workspaces/{ws}/decisions")
    assert response.status_code == 200, response.text
    rows = response.json()
    assert len(rows) == 300
    assert rows[0]["id"] == qid
    assert rows[0]["resolved_at"] is None and rows[0]["actionable"]
    assert rows[0]["judge"]["summary"] == "Needs a person"
    assert pending_id not in {row["id"] for row in rows}
    assert all(row["status"] == "resolved" for row in rows[1:])


def test_judge_metrics_cohorts_utc_and_empty(client, data, monkeypatch):
    from datetime import datetime, timezone
    from aifactory.decision_judges import judge_metrics

    ws = data[0]
    # Non-UTC caller instant normalizes to midnight UTC.
    now = datetime(2026, 11, 10, 2, tzinfo=timezone(timedelta(hours=2)))
    naive = datetime(2026, 11, 10)
    with db.session_scope() as s:
        assert judge_metrics(s, ws, now=now)["days_7"] == dict(decided=0, sent_back=0, held=0, overruled=0, overrule_rate=0)
        def add(days, verdict, *, mode="decide", state="completed", corrected=False, delta=timedelta(), workspace=ws):
            q = Decision(workspace_id=workspace, kind="question", title="Q")
            s.add(q)
            s.flush()
            s.add(DecisionReview(workspace_id=workspace, decision_id=q.id, state=state, mode=mode,
                effective_verdict=verdict, finished_at=naive - timedelta(days=days) + delta,
                correction_decision_id=data[-2] if corrected else None))
        add(7, "decide", corrected=True)
        add(7, "send_back", delta=timedelta(microseconds=-1))
        add(30, "send_back", corrected=True)
        add(30, "decide", delta=timedelta(microseconds=-1))
        add(0, "hold")
        add(1, "decide", mode="suggest")
        add(1, "decide", mode="off")
        add(1, "decide", state="pending")
        add(1, "decide", state="bypassed")
        add(0, "decide", delta=timedelta(microseconds=1))
        foreign = Workspace(org_id=s.get(Workspace, ws).org_id, name="Other", slug="other")
        s.add(foreign)
        s.flush()
        add(1, "decide", workspace=foreign.id)
        s.flush()
        stats = judge_metrics(s, ws, now=now)
        assert stats["days_7"] == dict(decided=1, sent_back=0, held=2, overruled=1, overrule_rate=1)
        assert stats["days_30"] == dict(decided=1, sent_back=2, held=2, overruled=2, overrule_rate=2/3)
    monkeypatch.setattr(db, "utcnow", lambda: naive)
    assert client.get(f"/api/workspaces/{ws}/stats").json()["decision_judges"] == stats
    with db.session_scope() as s:
        s.query(DecisionReview).filter(DecisionReview.mode == "decide").delete()
        s.flush()
        assert judge_metrics(s, ws, now=now)["days_7"]["overrule_rate"] == 0


@pytest.mark.parametrize("change", ["pending", "suggest", "off", "hold", "stale", "foreign_association"])
def test_only_current_automatic_verdict_can_be_overruled(client, data, change):
    automatic(data)
    with db.session_scope() as s:
        q = s.get(Decision, data[-1])
        if change == "pending":
            q.review.state, q.review.finished_at = "pending", None
        elif change in ("suggest", "off"):
            q.review.mode = change
        elif change == "hold":
            q.review.effective_verdict = "hold"
        elif change == "stale":
            q.review.question_fingerprint = question_fingerprint(q)
            q.body = "Changed after review"
        else:
            foreign = Workspace(org_id=s.get(Workspace, data[0]).org_id, name="Foreign", slug="foreign")
            s.add(foreign)
            s.flush()
            s.get(Task, data[4]).workspace_id = foreign.id
    response = client.post(f"/api/workspaces/{data[0]}/decisions/{data[-1]}/overrule",
        json={"resolution": "answer", "answer": "Correction", "reason": "Evidence"})
    assert response.status_code == 400, response.text
    with db.session_scope() as s:
        assert s.get(Decision, data[-1]).review.correction_decision_id is None
        assert s.query(Message).count() == 0
        assert s.query(Decision).count() == 2


def test_stale_session_cannot_overrule_twice(data):
    from aifactory.decisions import DecisionError, overrule
    from aifactory.models import User

    automatic(data)
    with db.session_scope() as stale:
        q = stale.get(Decision, data[-1])
        assert q.review.correction_decision_id is None
        stale.commit()
        with db.session_scope() as first:
            overrule(first, first.get(Decision, q.id), first.get(User, data[1]), "answer", "First", "Evidence")
        with pytest.raises(DecisionError):
            overrule(stale, q, stale.get(User, data[1]), "answer", "Second", "Other evidence")
    with db.session_scope() as s:
        assert s.query(Decision).count() == 3
        assert s.query(Message).count() == 1


def test_unblock_overrule_does_not_replay_retry(client, data):
    automatic(data, verdict="send_back", scope="item")
    with db.session_scope() as s:
        q = s.get(Decision, data[-1])
        q.kind, q.options, q.resolution = "unblock", [{"value": "retry"}], "retry"
        task = s.get(Task, data[4])
        task.attempt, task.stage, task.blocked_reason = 3, "verify", "Original reason"
    response = client.post(f"/api/workspaces/{data[0]}/decisions/{data[-1]}/overrule",
        json={"resolution": "retry", "answer": "Check the new docs", "reason": "Docs changed"})
    assert response.status_code == 200, response.text
    with db.session_scope() as s:
        task = s.get(Task, data[4])
        assert (task.status, task.stage, task.attempt, task.blocked_reason) == ("running", "verify", 3, "Original reason")
        correction = s.get(Decision, response.json()["correction_decision_id"])
        assert correction.scope == "item" and correction.reusable


@pytest.mark.parametrize("role,kind", [("planner", "question"), ("builder", "question"), ("verifier", "unblock")])
@pytest.mark.parametrize("mode,verdict", [("decide", "decide"), ("decide", "send_back"), ("decide", "hold"),
    ("suggest", "decide"), ("suggest", "send_back"), ("suggest", "hold"), ("off", "decide")])
def test_durable_review_end_to_end(data, client, monkeypatch, role, kind, mode, verdict):
    import asyncio
    import json
    from aifactory import decision_judge_runner as runner
    from aifactory.inference import ProviderConfig
    from aifactory.models import Run, User
    from aifactory.pipeline.orchestrator import Orchestrator
    from aifactory.pipeline.runtime import open_decision, guidance_history, take_guidance
    from aifactory.metrics import human_message
    from tests.fakes import ScriptedModel

    ws, _, _, outcome, task, prior, current = data
    target_task = None if role == "planner" else task
    with db.session_scope() as s:
        s.delete(s.get(Decision, current))
        s.get(Decision, prior).scope = "workspace"
        s.get(Workspace, ws).settings = {"decision_judges": mode, "models": {"judge": "judge-custom"},
            "pricing": {"judge-custom": {"input": 1, "output": 2}}}
        s.get(Outcome, outcome).status = "needs_input" if role == "planner" else "running"
        s.get(Task, task).status = "needs_input"
    ScriptedModel.judge_calls = []
    ScriptedModel.scripts = {"judge": lambda _: {"content": json.dumps({**report(prior), "verdict": verdict,
        "resolution": "retry" if kind == "unblock" else "answer"})}}
    monkeypatch.setattr(runner, "ModelClient", ScriptedModel)
    monkeypatch.setattr(runner, "resolve_provider", lambda _: ProviderConfig("mindshub", "unused", "secret", "Test"))
    options = [{"value": "retry" if kind == "unblock" else "answer"}, {"value": "cancel"}]
    if role == "planner":
        options.append({"value": "approve"})
    since = db.utcnow()
    did = open_decision(ws, kind, "Which queue?", "Need a queue", outcome_id=outcome, task_id=target_task,
                        options=options, metadata=metadata(role))
    assert open_decision(ws, kind, "Which queue?", "Need a queue", outcome_id=outcome, task_id=target_task,
                         options=options, metadata=metadata(role)) == did
    inbox = lambda: client.get(f"/api/workspaces/{ws}/inbox").json()["decisions"]
    assert [d["id"] for d in inbox()] == ([did] if mode == "off" else [])

    async def drain():
        orch = Orchestrator()
        orch._schedule_reviews()
        orch._schedule_reviews()
        await asyncio.gather(*orch._reviews.values())
        await asyncio.sleep(0)  # run done callbacks, no wall-clock wait
        orch._schedule_reviews()
        assert not orch._reviews
    asyncio.run(drain())
    auto = mode == "decide" and verdict != "hold"
    assert len(ScriptedModel.judge_calls) == (0 if mode == "off" else 1)
    assert [d["id"] for d in inbox()] == ([] if auto else [did])
    with db.session_scope() as s:
        d = s.get(Decision, did)
        assert d.status == ("resolved" if auto else "open")
        assert s.query(Run).filter_by(role="judge").count() == (0 if mode == "off" else 1)
        if mode != "off":
            assert d.review.finished_at and serializers.actionable(d) == (not auto)
            assert d.review.proposed_verdict == verdict
        if auto:
            assert s.get(User, d.resolved_by).name == "Decision judge"
            assert d.resolution == ("send_back" if verdict == "send_back" else options[0]["value"])
            assert d.reusable == (verdict == "decide")
            messages = s.query(Message).filter_by(outcome_id=outcome).all()
            assert len(messages) == 1 and not human_message(messages[0])
            assert serializers.message(messages[0])["author"] == "Decision judge"
            assert (s.get(Outcome, outcome).status == "planning" if role == "planner"
                    else s.get(Task, task).status == "queued")
    assert guidance_history(outcome, target_task) == (["Use the durable queue."] if auto else [])
    take = take_guidance(outcome, target_task, since)
    assert take() == (["Use the durable queue."] if auto else [])
    assert take() == []
    if mode == "suggest":
        # A proposal is inert until a member explicitly chooses the safe action.
        response = client.post(f"/api/workspaces/{ws}/decisions/{did}/resolve", json={
            "resolution": options[0]["value"], "comment": "Confirmed by the team."})
        assert response.status_code == 200, response.text
        with db.session_scope() as s:
            d = s.get(Decision, did)
            assert d.status == "resolved" and d.resolved_by == data[1]
            messages = s.query(Message).filter_by(outcome_id=outcome).all()
            assert len(messages) == 1 and human_message(messages[0])
            assert (s.get(Outcome, outcome).status == "planning" if role == "planner"
                    else s.get(Task, task).status == "queued")
        assert take() == ["Confirmed by the team."]
        assert take() == []


def test_review_restart_edit_idempotence_and_stale_result(data):
    from aifactory.models import Run
    from aifactory.pipeline.decision_reviews import apply, claim, recover
    from aifactory.pipeline.runtime import open_decision

    ws, _, _, outcome, task, prior, current = data
    def opening(body):
        return open_decision(ws, "question", "Which queue?", body, outcome_id=outcome, task_id=task,
                             options=[{"value": "answer"}], metadata=metadata())
    opening("Original")
    with db.session_scope() as s:
        rid = s.query(DecisionReview).filter_by(decision_id=current).one().id
    first = claim(rid)
    assert claim(rid) is None
    with db.session_scope() as s:
        q = s.get(Decision, current)
        run = Run(workspace_id=ws, outcome_id=outcome, harness="control_plane", role="judge", status="succeeded", config_snapshot={"context": build_context(s, q)})
        s.add(run)
        s.flush()
    with db.session_scope() as s:
        recover(s)
    second = claim(rid)
    apply(rid, first, run, report(prior))
    with db.session_scope() as s:
        assert s.get(Decision, current).status == "judging"
    opening("Edited")
    apply(rid, second, run, report(prior))
    with db.session_scope() as s:
        assert s.query(Message).count() == 0
        assert s.get(DecisionReview, rid).state == "pending"
    third = claim(rid)
    # Even a direct question edit (not open_decision) invalidates the result.
    with db.session_scope() as s:
        s.get(Decision, current).body = "Edited again"
    apply(rid, third, run, report(prior))
    fourth = claim(rid)
    with db.session_scope() as s:
        run.config_snapshot = {"context": build_context(s, s.get(Decision, current))}
    apply(rid, fourth, run, report(prior))
    apply(rid, fourth, run, report(prior))
    with db.session_scope() as s:
        assert s.get(Decision, current).status == "resolved"
        assert s.query(Message).count() == 1


@pytest.mark.parametrize("running", [False, True])
@pytest.mark.parametrize("target", ["outcome", "task"])
def test_cancellation_stops_review_after_decision_superseded(data, monkeypatch, running, target):
    import asyncio
    from aifactory import decision_judge_runner as runner, work_services
    from aifactory.models import User
    from aifactory.pipeline.orchestrator import Orchestrator
    from aifactory.pipeline.runtime import open_decision

    ws, uid, _, outcome, task, _, current = data
    open_decision(ws, "question", "Queue?", "Body", outcome_id=outcome, task_id=task,
                  options=[{"value": "answer"}], metadata=metadata())
    entered, cancelled = asyncio.Event(), asyncio.Event()
    async def blocked(*args):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
    monkeypatch.setattr(runner, "run_judge", blocked)
    async def exercise():
        orch = Orchestrator()
        if running:
            orch._schedule_reviews()
            await entered.wait()
        with db.session_scope() as s:
            if target == "outcome":
                work_services.cancel_outcome(s, s.get(User, uid), ws, outcome)
            else:
                work_services.cancel_task(s, s.get(User, uid), ws, task)
        # Real follow-up runs only after the cancellation transaction commits.
        if target == "outcome":
            orch.cancel_outcome(outcome)
        else:
            orch.cancel_task(task)
        jobs = list(orch._reviews.values())
        await asyncio.gather(*jobs, return_exceptions=True)
        orch._schedule_reviews()
        assert cancelled.is_set() == running
        assert entered.is_set() == running
    asyncio.run(exercise())
    with db.session_scope() as s:
        assert s.get(Decision, current).status == "superseded"
        assert s.query(Message).count() == 0
        assert s.get(Task, task).status == "cancelled"
        assert (s.get(Outcome, outcome).status == "cancelled") == (target == "outcome")
        r = s.query(DecisionReview).filter_by(decision_id=current).one()
        assert r.state == "superseded" and r.finished_at


@pytest.mark.parametrize("scope", ["item", "repository"])
def test_reject_citations_outside_recorded_scope(data, scope):
    ws, _, _, outcome, task, prior, current = data
    with db.session_scope() as s:
        source = s.get(Decision, prior)
        source.scope = scope
        source.task_id = source.outcome_id = None
        q = s.get(Decision, current)
        with pytest.raises(ValueError, match="scope"):
            validate_report(s, q, report(prior), build_context(s, q))


@pytest.mark.parametrize("kind,flags,options", [
    ("approve_plan", {}, ["approve"]), ("approve_merge", {}, ["approve"]),
    ("budget", {"budget": True}, ["retry"]),
    ("unblock", {"takeover": True}, ["retry", "take_over"]),
    ("question", {"protected_paths": True}, ["answer"]),
    ("question", {"paths": ["deploy/prod.yml"]}, ["answer"]),
])
def test_runtime_governance_bypasses_judge_and_remains_human_resolvable(data, client, monkeypatch, kind, flags, options):
    import asyncio
    from aifactory import decision_judge_runner as runner
    from aifactory.pipeline.runtime import open_decision
    from aifactory.pipeline.orchestrator import Orchestrator

    ws, _, _, outcome, task, _, current = data
    def forbidden(*args):
        pytest.fail("governance reached judge")
    monkeypatch.setattr(runner, "run_judge", forbidden)
    did = open_decision(ws, kind, "Human required", "Governance", outcome_id=outcome, task_id=task,
                        options=[{"value": v} for v in options], metadata=metadata(**flags))
    async def tick():
        orch = Orchestrator()
        orch._schedule_reviews()
        assert not orch._reviews
    asyncio.run(tick())
    with db.session_scope() as s:
        d = s.get(Decision, did)
        assert serializers.actionable(d) and d.review is None
    # Governance still uses the decision's explicit human choices; dismiss is
    # intentionally invalid when it is not offered (it would strand the work).
    resolution = options[0]
    response = client.post(f"/api/workspaces/{ws}/decisions/{did}/resolve",
                           json={"resolution": resolution, "comment": "Human guidance"})
    assert response.status_code == 200, response.text
    with db.session_scope() as s:
        d = s.get(Decision, did)
        assert d.status == "resolved" and d.resolution == resolution
        assert d.resolved_by == data[1] and d.review is None


def test_shutdown_before_worker_starts_releases_claim(data):
    import asyncio
    from aifactory.pipeline.orchestrator import Orchestrator
    from aifactory.pipeline.runtime import open_decision

    ws, _, _, outcome, task, _, current = data
    open_decision(ws, "question", "Queue?", "Body", outcome_id=outcome, task_id=task,
                  options=[{"value": "answer"}], metadata=metadata())
    async def exercise():
        orch = Orchestrator()
        orch._schedule_reviews()
        await orch.stop()
    asyncio.run(exercise())
    with db.session_scope() as s:
        r = s.query(DecisionReview).filter_by(decision_id=current).one()
        assert r.state == "pending"
        assert s.get(Decision, current).status == "judging"


def test_suggest_requires_a_person_even_when_lead_is_enabled(data, client):
    from aifactory.pipeline.lead import Tools

    ws, _, _, outcome, task, _, current = data
    with db.session_scope() as s:
        s.get(Task, task).status = "needs_input"
        s.add(DecisionReview(workspace_id=ws, decision_id=current, mode="suggest", state="finished",
            finished_at=db.utcnow(), proposed_verdict="decide", effective_verdict="hold",
            answer="Proposed answer", summary="Confirm the proposal"))
    lead = Tools(ws, outcome, "lead-run")
    result = lead._resolve_decision(current, "answer", "Proposed answer")
    assert "requires human confirmation" in result
    with db.session_scope() as s:
        assert s.get(Decision, current).status == "open"
        assert s.get(Task, task).status == "needs_input"
        assert s.query(Message).count() == 0
    response = client.post(f"/api/workspaces/{ws}/decisions/{current}/resolve",
                           json={"resolution": "answer", "comment": "Confirmed answer"})
    assert response.status_code == 200, response.text
    with db.session_scope() as s:
        assert s.get(Task, task).status == "queued"
        assert s.query(Message).one().author_kind == "user"


@pytest.mark.parametrize("scope", ["item", "repository"])
def test_citation_from_another_task_or_repository_is_held_at_apply(data, scope):
    from aifactory.models import Run
    from aifactory.pipeline.decision_reviews import apply, claim
    from aifactory.pipeline.runtime import open_decision

    ws, _, _, outcome, task, prior, current = data
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="team/other")
        s.add(repo)
        s.flush()
        other = Task(workspace_id=ws, outcome_id=outcome, repo_id=repo.id,
                     plan_id=s.get(Task, task).plan_id, key="T2", title="Other")
        s.add(other)
        s.flush()
        source = s.get(Decision, prior)
        source.scope, source.task_id = scope, other.id
    open_decision(ws, "question", "Which queue?", "Need an answer", outcome_id=outcome, task_id=task,
                  options=[{"value": "answer"}], metadata=metadata())
    with db.session_scope() as s:
        rid = s.query(DecisionReview).filter_by(decision_id=current).one().id
        run = Run(workspace_id=ws, outcome_id=outcome, role="judge", harness="control_plane", status="succeeded",
                  config_snapshot={"context": build_context(s, s.get(Decision, current))})
        s.add(run)
        s.flush()
    token = claim(rid)
    apply(rid, token, run, report(prior))
    apply(rid, token, run, report(prior))
    with db.session_scope() as s:
        question = s.get(Decision, current)
        assert question.status == "open" and serializers.actionable(question)
        assert question.review.effective_verdict == "hold"
        assert "scope" in question.review.summary
        assert question.review.finished_at
        assert s.query(Message).count() == 0


def test_cancelled_outcome_is_never_claimed_even_without_followup(data):
    from aifactory.models import User
    from aifactory.pipeline.decision_reviews import claim
    from aifactory.pipeline.runtime import open_decision
    from aifactory.work_services import cancel_outcome

    ws, uid, _, outcome, task, _, current = data
    open_decision(ws, "question", "Queue?", "Body", outcome_id=outcome, task_id=task,
                  options=[{"value": "answer"}], metadata=metadata())
    with db.session_scope() as s:
        rid = s.query(DecisionReview).filter_by(decision_id=current).one().id
        cancel_outcome(s, s.get(User, uid), ws, outcome)
    assert claim(rid) is None
    with db.session_scope() as s:
        assert s.get(Decision, current).status == "superseded"
        assert s.get(DecisionReview, rid).state == "superseded"
        assert s.get(DecisionReview, rid).finished_at
        assert s.query(Message).count() == 0


@pytest.mark.parametrize("started", [False, True])
def test_review_workers_are_bounded_and_shutdown_preserves_pending(data, monkeypatch, started):
    import asyncio
    from types import SimpleNamespace
    from aifactory import decision_judge_runner as runner
    from aifactory.pipeline import orchestrator as scheduling
    from aifactory.pipeline.runtime import open_decision

    ws, _, _, outcome, _, _, _ = data
    # Separate outcomes represent independent durable work, not duplicate questions.
    with db.session_scope() as s:
        repo_ids = s.get(Outcome, outcome).repo_ids
        outcomes = [Outcome(workspace_id=ws, number=100 + i, title=f"Review {i}",
                            repo_ids=repo_ids, status="needs_input") for i in range(5)]
        s.add_all(outcomes)
        s.flush()
        ids = [o.id for o in outcomes]
    for oid in ids:
        open_decision(ws, "question", "Queue?", "Body", outcome_id=oid,
                      options=[{"value": "answer"}], metadata=metadata("planner"))
    monkeypatch.setattr(scheduling, "get_settings", lambda: SimpleNamespace(global_concurrency=2))
    entered = []

    async def blocked(*args):
        entered.append(args)
        await asyncio.Event().wait()

    monkeypatch.setattr(runner, "run_judge", blocked)

    async def exercise():
        orch = scheduling.Orchestrator()
        orch._schedule_reviews()
        orch._schedule_reviews()
        assert len(orch._reviews) == 2
        with db.session_scope() as s:
            assert s.query(DecisionReview).filter_by(state="running").count() == 2
            assert s.query(DecisionReview).filter_by(state="pending").count() == 3
        if started:
            await asyncio.sleep(0)
        await orch.stop()
        assert len(entered) == (2 if started else 0)
        assert not orch._reviews
        orch._schedule_reviews()
        assert not orch._reviews

    asyncio.run(exercise())
    with db.session_scope() as s:
        assert s.query(DecisionReview).filter_by(state="pending").count() == 5
        assert s.query(DecisionReview).filter_by(state="running").count() == 0
        assert s.query(Decision).filter(Decision.outcome_id.in_(ids), Decision.status == "judging").count() == 5
        assert s.query(Message).count() == 0


def test_judging_and_review_commit_before_publication(data, monkeypatch):
    from aifactory import events
    from aifactory.pipeline import runtime
    from aifactory.pipeline import decision_reviews

    ws, _, _, outcome, task, _, current = data
    original = runtime.publish_decision
    observed, published = [], []
    monkeypatch.setattr(events.bus, "publish", published.append)

    def inspect_committed(*args, **kwargs):
        # The publisher uses a fresh transaction, like an HTTP reader/SSE consumer.
        with db.session_scope() as s:
            q = s.get(Decision, current)
            r = s.query(DecisionReview).filter_by(decision_id=current).one()
            observed.append((q.status, r.state, r.question_fingerprint))
        original(*args, **kwargs)

    monkeypatch.setattr(runtime, "publish_decision", inspect_committed)
    runtime.open_decision(ws, "question", "Queue?", "Body", outcome_id=outcome, task_id=task,
                          options=[{"value": "answer"}], metadata=metadata())
    assert len(observed) == 1 and observed[0][:2] == ("judging", "pending")
    assert observed[0][2]
    assert published == []  # No transient human-attention event before the review.

    # Failure to persist the review must roll back the question revision as well.
    def fail(*args):
        raise RuntimeError("Cannot persist review")

    monkeypatch.setattr(decision_reviews, "prepare", fail)
    with pytest.raises(RuntimeError, match="Cannot persist review"):
        runtime.open_decision(ws, "question", "New title", "New body", outcome_id=outcome, task_id=task,
                              options=[{"value": "answer"}], metadata=metadata())
    with db.session_scope() as s:
        assert s.get(Decision, current).body == "Body"
        assert s.get(Decision, current).review.state == "pending"
    assert len(observed) == 1 and published == []


def test_pending_review_prevents_scheduler_missing_decision_fallback(data):
    from aifactory.pipeline.orchestrator import Orchestrator, UNASKED_AFTER
    from aifactory.pipeline.runtime import open_decision
    import time

    ws, _, _, outcome, task, _, current = data
    with db.session_scope() as s:
        s.get(Outcome, outcome).status = "running"
        s.get(Task, task).status = "needs_input"
    open_decision(ws, "question", "Queue?", "Body", outcome_id=outcome, task_id=task,
                  options=[{"value": "answer"}], metadata=metadata())
    orch = Orchestrator()
    orch._unasked[task] = time.monotonic() - UNASKED_AFTER - 1
    orch._promote()
    with db.session_scope() as s:
        waiting = s.query(Decision).filter(Decision.outcome_id == outcome,
                                         Decision.status.in_(("open", "judging"))).all()
        assert [d.id for d in waiting] == [current]
        assert waiting[0].status == "judging"
