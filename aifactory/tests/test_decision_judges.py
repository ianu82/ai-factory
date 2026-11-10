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
                                  resolution="answer", comment="Use a database.", resolved_by=data[1],
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
