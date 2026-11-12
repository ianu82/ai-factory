"""Offline adapter contracts, secret boundaries, install layout and durable tenancy."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest
from sqlalchemy import inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import StaleDataError

from aifactory import benchmarks as b, db
from aifactory.models import (
    EvalCaseResult, EvalConfiguration, EvalDailyBudget, EvalRun, EvalSchedule,
    Organization, Outcome, Task, Workspace,
)


def workspace():
    with db.session_scope() as s:
        org = Organization(name="test")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="test", slug="test")
        s.add(ws)
        s.flush()
        return ws.id


def configuration(ws, **kwargs):
    return b.snapshot_configuration(ws, **({"settings": {}, "prompts": {"builder": "Implement the task"},
                                           "prompt_version": "v1", "execution": {"sandbox_kind": "vm"}} | kwargs))


def suite_directory(tmp_path):
    root = tmp_path / "suite"
    shutil.copytree(Path(b.__file__).parent / "eval_data/smoke", root)
    return root


def test_offline_suite_and_scorer(monkeypatch):
    import socket

    def forbidden(*args, **kwargs):
        raise AssertionError("network access")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    suite = b.load_eval_suite()
    assert suite == b.load_eval_suite("smoke")
    case = suite.cases[0]
    assert case.fixtures["calculator.py"].startswith("def add")
    assert len(case.revision) == len(suite.revision) == 64
    snapshot = suite.snapshot()
    snapshot["cases"][0]["fixtures"]["calculator.py"] = "changed"
    assert suite.revision == b.load_eval_suite().revision
    scorer = b.get_scorer("checks-v1")
    for evidence, verdict in [({}, "error"), ({"checks": []}, "error"),
                              ({"checks": [{"command": case.check_commands[0], "exit_code": 0, "timed_out": True}]}, "fail"),
                              ({"checks": [{"command": "wrong", "exit_code": 0}]}, "error"),
                              ({"checks": [{"command": case.check_commands[0], "exit_code": True}]}, "error"),
                              ({"checks": [{"command": case.check_commands[0], "exit_code": 1}]}, "fail"),
                              ({"checks": [{"command": case.check_commands[0], "exit_code": 0}]}, "pass")]:
        score = scorer(case, evidence)
        assert score.verdict == verdict and score.explanation
        assert json.loads(score.model_dump_json())["scorer_id"] == "checks-v1"
    with pytest.raises(b.SuiteError):
        b.get_scorer("unknown")
    with pytest.raises(b.SuiteError):
        b.load_eval_suite("unknown")


@pytest.mark.parametrize("field", ["source", "destination"])
@pytest.mark.parametrize("path", ["/etc/passwd", "../outside", "a/../../b", "C:/windows", "a\\b", "./a", "a//b"])
def test_reject_unsafe_paths(tmp_path, field, path):
    root = suite_directory(tmp_path)
    raw = json.loads((root / "suite.json").read_text())
    raw["cases"][0]["fixtures"] = {"ok.py": path} if field == "source" else {path: "fixtures/calculator.py"}
    (root / "suite.json").write_text(json.dumps(raw))
    with pytest.raises(b.SuiteError):
        b.load_eval_suite(root=root)


@pytest.mark.parametrize("target", ["suite.json", "fixtures", "fixtures/calculator.py", "."])
def test_reject_symlinks(tmp_path, target):
    root = suite_directory(tmp_path)
    path = root if target == "." else root / target
    moved = tmp_path / "moved"
    path.rename(moved)
    path.symlink_to(moved, target_is_directory=moved.is_dir())
    with pytest.raises(b.SuiteError):
        b.load_eval_suite(root=root)


def test_unknown_scorer_and_content_revisions(tmp_path):
    root = suite_directory(tmp_path)
    first = b.load_eval_suite(root=root)
    (root / "fixtures/calculator.py").write_text("# changed fixture\n")
    second = b.load_eval_suite(root=root)
    assert first.revision != second.revision
    assert first.cases[0].revision != second.cases[0].revision
    raw = json.loads((root / "suite.json").read_text())
    raw["cases"][0]["scorer_ids"] = ["unknown"]
    (root / "suite.json").write_text(json.dumps(raw))
    with pytest.raises(b.SuiteError):
        b.load_eval_suite(root=root)


def test_bounded_suite_reads(tmp_path):
    root = suite_directory(tmp_path)
    (root / "fixtures/calculator.py").write_bytes(b"x" * (b.MAX_FIXTURE_BYTES + 1))
    with pytest.raises(b.SuiteError):
        b.load_eval_suite(root=root)
    (root / "suite.json").write_bytes(b" " * (b.MAX_SUITE_BYTES + 1))
    with pytest.raises(b.SuiteError):
        b.load_eval_suite(root=root)


def test_installed_layout_without_checkout(tmp_path):
    """The package directory alone, just like site-packages, contains the suite."""
    installed = tmp_path / "site-packages"
    shutil.copytree(Path(b.__file__).parent, installed / "aifactory", ignore=shutil.ignore_patterns("__pycache__"))
    env = {**os.environ, "PYTHONPATH": str(installed)}
    result = subprocess.run([sys.executable, "-c", "from aifactory.benchmarks import load_eval_suite; "
                             "s=load_eval_suite(); assert s.cases[0].fixtures; print(s.revision)"],
                            cwd=tmp_path, env=env, capture_output=True, text=True, check=True)
    assert result.stdout.strip() == b.load_eval_suite().revision


def test_configuration_stability_and_immutability():
    ws = workspace()
    first = configuration(ws)
    assert configuration(ws).id == first.id
    assert configuration(ws, prompts={"builder": "Different instructions"}).id != first.id
    assert configuration(ws, prompt_version="v2").id != first.id
    assert configuration(ws, settings={"max_attempts": 4}).id != first.id
    assert configuration(ws, execution={"sandbox_kind": "vm", "cpu": 8}).id != first.id
    assert configuration(ws, settings={"budgets": {"run_tokens": 12}}).id != configuration(
        ws, settings={"budgets": {"run_tokens": 24}}).id
    assert configuration(ws, settings={"budgets": {"run_tokens": 12}}).snapshot["settings"]["budgets"]["run_tokens"] == 12
    with pytest.raises(ValueError, match="immutable"), db.session_scope() as s:
        s.get(EvalConfiguration, (ws, first.id)).snapshot = {"changed": True}
        s.flush()
    assert b.get_configuration(ws, first.id).snapshot == first.snapshot
    with pytest.raises(b.SuiteError):
        b.get_configuration(workspace(), first.id)
    with pytest.raises(b.SuiteError):
        b.get_configuration(ws, "unknown")


@pytest.mark.parametrize("branch", ["qa", "build"])
@pytest.mark.parametrize("key", ["accessToken", "access-token", "access_token", "ACCESS_TOKEN",
                                "apiKey", "api-key", "api_key", "API_KEY", "feedbackKey"])
def test_normalized_credentials_never_reach_snapshot_or_hash(branch, key):
    ws = workspace()
    def snapshot(secret):
        return configuration(ws, settings={branch: {key: secret, "start": "run tests"}},
                             prompts={"builder": "run tests"})
    first, second = snapshot("first-credential-value"), snapshot("second-credential-value")
    assert first.id == second.id
    raw = json.dumps(first.snapshot)
    assert "first-credential-value" not in raw and key not in first.snapshot["settings"][branch]
    assert first.snapshot["prompts"]["builder"] == "run tests"
    assert first.snapshot["settings"][branch]["start"] == "run tests"


def test_secrets_do_not_remove_public_keys(monkeypatch):
    ws = workspace()
    monkeypatch.setenv("SOME_SECRET", "run")
    cfg = configuration(ws, settings={"budgets": {"run_tokens": 123}, "models": {"builder": "fake"}},
                        credential_values=("model",))
    assert cfg.snapshot["settings"]["models"]["builder"] == "fake"
    assert cfg.snapshot["settings"]["budgets"]["run_tokens"] == 123
    assert "sandbox_kind" in cfg.snapshot["execution"]
    assert cfg.id != configuration(ws, settings={"budgets": {"run_tokens": 124}}, credential_values=("model",)).id


def test_secret_environment_and_nested_values(monkeypatch):
    ws = workspace()
    monkeypatch.setenv("EVAL_API_KEY", "from-process-env")
    cfg = configuration(ws, execution={"env": {"CUSTOM": "secret-env-value"},
                                      "password": "nested-pass", "start": "run tests"})
    raw = json.dumps(cfg.snapshot)
    assert "env" not in cfg.snapshot["execution"]
    for secret in ("from-process-env", "secret-env-value", "nested-pass"):
        assert secret not in raw
    assert cfg.snapshot["execution"]["start"] == "run tests"


def run_values(ws, config_id, **kwargs):
    suite = b.load_eval_suite()
    return dict(workspace_id=ws, invocation_id="launch", suite_id=suite.id, suite_revision=suite.revision,
                suite_snapshot=suite.snapshot(), worker="native", model="fake", configuration_id=config_id) | kwargs


def case_values(ws, run_id, **kwargs):
    case = b.load_eval_suite().cases[0]
    return dict(workspace_id=ws, run_id=run_id, case_id=case.id, case_revision=case.revision,
                case_snapshot=case.snapshot()) | kwargs


def test_persistence_unique_cells_cases_and_no_delivery_records():
    ws = workspace()
    cfg = configuration(ws)
    with db.session_scope() as s:
        schedule = EvalSchedule(workspace_id=ws)
        s.add(schedule)
        s.flush()
        schedule_id = schedule.id
        row = EvalRun(**run_values(ws, cfg.id, schedule_id=schedule.id, period="2026-11-11"))
        s.add(row)
        s.flush()
        run_id = row.id
        s.add(EvalCaseResult(**case_values(ws, run_id, verdict="pass", scores=[{"explanation": "passed"}],
                                         usage={"input_tokens": 5, "output_tokens": 2}, cost_known=True)))
        s.add(EvalDailyBudget(workspace_id=ws, day="2026-11-11", limit_usd=10, reserved_usd=1))
    db.init_engine()
    db.init_engine()
    with db.session_scope() as s:
        assert s.get(EvalRun, run_id).suite_snapshot == b.load_eval_suite().snapshot()
        assert s.scalar(select(EvalCaseResult).where(EvalCaseResult.run_id == run_id)).usage["input_tokens"] == 5
        assert s.get(EvalDailyBudget, (ws, "2026-11-11")).reserved_usd == 1
        assert s.query(Outcome).count() == s.query(Task).count() == 0
    with pytest.raises(IntegrityError), db.session_scope() as s:
        s.add(EvalRun(**run_values(ws, cfg.id, schedule_id=schedule_id, period="2026-11-11")))
    with pytest.raises(IntegrityError), db.session_scope() as s:
        s.add(EvalCaseResult(**case_values(ws, run_id)))
    # On demand invocations are intentionally repeatable, but share a group ID.
    with db.session_scope() as s:
        s.add_all([EvalRun(**run_values(ws, cfg.id)) for _ in range(2)])
    with pytest.raises(ValueError, match="immutable"), db.session_scope() as s:
        s.get(EvalRun, run_id).suite_revision = "changed"
    with pytest.raises(ValueError, match="immutable"), db.session_scope() as s:
        s.scalar(select(EvalCaseResult).where(EvalCaseResult.run_id == run_id)).case_revision = "changed"


def test_cross_workspace_links_rejected():
    ws, other = workspace(), workspace()
    cfg = configuration(ws)
    with pytest.raises(IntegrityError), db.session_scope() as s:
        s.add(EvalRun(**run_values(other, cfg.id)))
    other_cfg = configuration(other)
    with db.session_scope() as s:
        schedule = EvalSchedule(workspace_id=ws)
        s.add(schedule)
        s.flush()
        schedule_id = schedule.id
        run = EvalRun(**run_values(ws, cfg.id))
        s.add(run)
        s.flush()
        run_id = run.id
    with pytest.raises(IntegrityError), db.session_scope() as s:
        s.add(EvalRun(**run_values(other, other_cfg.id, schedule_id=schedule_id, period="2026-11-11")))
    with pytest.raises(IntegrityError), db.session_scope() as s:
        s.add(EvalRun(**run_values(other, other_cfg.id, previous_run_id=run_id)))
    with pytest.raises(IntegrityError), db.session_scope() as s:
        s.add(EvalCaseResult(**case_values(other, run_id)))


@pytest.mark.parametrize("thresholds", [None, {"custom": 1}])
def test_default_thresholds(thresholds):
    ws = workspace()
    with db.session_scope() as s:
        schedule = EvalSchedule(workspace_id=ws, thresholds=thresholds)
        s.add(schedule)
        s.flush()
        assert schedule.thresholds == (thresholds or b.DEFAULT_THRESHOLDS)


@pytest.mark.parametrize("kind", [EvalSchedule, EvalRun, EvalCaseResult])
def test_stale_claimant_is_fenced(kind):
    ws = workspace()
    cfg = configuration(ws)
    with db.session_scope() as s:
        schedule = EvalSchedule(workspace_id=ws)
        s.add(schedule)
        s.flush()
        run = EvalRun(**run_values(ws, cfg.id))
        s.add(run)
        s.flush()
        case = EvalCaseResult(**case_values(ws, run.id))
        s.add(case)
        s.flush()
        row_id = {EvalSchedule: schedule.id, EvalRun: run.id, EvalCaseResult: case.id}[kind]
    with Session(db.engine(), expire_on_commit=False) as stale:
        original = stale.get(kind, row_id)
        generation = original.claim_generation
        stale.commit()  # no long-lived read lock across another claimant
        with db.session_scope() as current:
            reclaimed = current.get(kind, row_id)
            reclaimed.claim_owner = "replacement"
            # Even reclaims with the same owner must explicitly advance the fence.
            reclaimed.claim_generation += 1
        if kind is EvalSchedule:
            original.last_period = "stale-period"
        else:
            original.status = "running"
        with pytest.raises(StaleDataError):
            stale.commit()
    with db.session_scope() as s:
        current = s.get(kind, row_id)
        assert current.claim_generation == generation + 1
        assert current.claim_owner == "replacement"
        if kind is EvalSchedule:
            assert current.last_period is None


def test_additive_creation_preserves_existing_delivery_schema():
    engine = db.engine()
    before = inspect(engine).get_columns("runs")
    # Simulate the last release, before these new tables existed.
    for table in [EvalCaseResult.__table__, EvalRun.__table__, EvalSchedule.__table__,
                  EvalDailyBudget.__table__, EvalConfiguration.__table__]:
        table.drop(engine)
    db.init_engine()
    after = inspect(db.engine()).get_columns("runs")
    assert [(c["name"], c["nullable"]) for c in before] == [(c["name"], c["nullable"]) for c in after]
    for table in (EvalSchedule, EvalRun, EvalCaseResult, EvalDailyBudget, EvalConfiguration):
        assert inspect(db.engine()).has_table(table.__tablename__)


def test_omitted_thresholds_use_contract_defaults():
    ws = workspace()
    with db.session_scope() as s:
        row = EvalSchedule(workspace_id=ws)
        s.add(row)
        s.flush()
        assert row.thresholds == b.DEFAULT_THRESHOLDS


def test_configuration_input_limits():
    ws = workspace()
    with pytest.raises(b.SuiteError):
        configuration(ws, execution={"text": "x" * (b.MAX_SUITE_BYTES + 1)})
    deep = {}
    for _ in range(32):
        deep = {"nested": deep}
    with pytest.raises(b.SuiteError):
        configuration(ws, execution=deep)


def test_smoke_fixtures_check_real_behavior(tmp_path):
    """Verify fixture checks fail before repair and pass after, without a model."""
    case = b.load_eval_suite().cases[0]
    for name, content in case.fixtures.items():
        (tmp_path / name).write_text(content)
    command = [sys.executable, "-m", "unittest", "-q", "test_calculator"]
    initial = subprocess.run(command, cwd=tmp_path, capture_output=True, timeout=10)
    assert initial.returncode != 0
    (tmp_path / "calculator.py").write_text("def add(a, b):\n    return a + b\n")
    fixed = subprocess.run(command, cwd=tmp_path, capture_output=True, timeout=10)
    assert fixed.returncode == 0
    score = b.get_scorer("checks-v1")(case, {"checks": [{
        "command": case.check_commands[0], "exit_code": fixed.returncode,
    }]})
    assert score.verdict == "pass"


@pytest.mark.parametrize("branch", ["qa", "build"])
@pytest.mark.parametrize("key", ["sort_key", "cache_key", "partition_key", "sortKey", "sort-key"])
def test_public_key_settings_are_versioned(branch, key):
    ws = workspace()
    first = configuration(ws, settings={branch: {key: "ascending"}})
    second = configuration(ws, settings={branch: {key: "descending"}})
    assert first.snapshot["settings"][branch][key] == "ascending"
    assert second.snapshot["settings"][branch][key] == "descending"
    assert first.id != second.id


@pytest.mark.parametrize("branch", ["qa", "build"])
@pytest.mark.parametrize("key", ["privateKey", "signing-key", "encryption_key", "AWS_ACCESS_KEY", "clientKey",
                                "subscription_key", "provider_api_key"])
def test_credential_key_fields_still_removed(branch, key):
    ws = workspace()
    first = configuration(ws, settings={branch: {key: "first-private-value"}})
    second = configuration(ws, settings={branch: {key: "second-private-value"}})
    assert first.id == second.id
    assert key not in first.snapshot["settings"][branch]
    assert "first-private-value" not in json.dumps(first.snapshot)


@pytest.mark.parametrize("status", ["queued", "running", "passed", "failed", "error", "cancelled", "budget_exceeded"])
def test_case_status_contract(status):
    ws = workspace()
    cfg = configuration(ws)
    with db.session_scope() as s:
        run = EvalRun(**run_values(ws, cfg.id))
        s.add(run)
        s.flush()
        case = EvalCaseResult(**case_values(ws, run.id, status=status))
        s.add(case)
        s.flush()
        case_id = case.id
    with db.session_scope() as s:
        assert s.get(EvalCaseResult, case_id).status == status


def test_invalid_case_status_rejected_by_orm_and_database():
    ws = workspace()
    cfg = configuration(ws)
    with db.session_scope() as s:
        run = EvalRun(**run_values(ws, cfg.id))
        s.add(run)
        s.flush()
        with pytest.raises(ValueError, match="Invalid eval case status"):
            EvalCaseResult(**case_values(ws, run.id, status="passsed"))
        case = EvalCaseResult(**case_values(ws, run.id))
        s.add(case)
        s.flush()
        case_id = case.id
        with pytest.raises(ValueError, match="Invalid eval case status"):
            case.status = "passsed"
    # Core/bulk writes bypass ORM validators, but cannot bypass the constraint.
    with pytest.raises(IntegrityError), db.engine().begin() as conn:
        conn.execute(EvalCaseResult.__table__.update().where(
            EvalCaseResult.id == case_id).values(status="passsed"))
    with db.session_scope() as s:
        assert s.get(EvalCaseResult, case_id).status == "queued"


@pytest.mark.parametrize("source", ["settings", "execution", "env", "explicit"])
@pytest.mark.parametrize("secret", ["run", "model", "run tests"])
def test_credential_rotation_preserves_public_text_and_identity(monkeypatch, source, secret):
    """Credentials must not redact incidental matches in public execution inputs."""
    ws = workspace()
    public_settings = {"qa": {"start": "run tests", "sort_key": "model"},
                       "build": {"start": "run tests", "sort_key": "model"},
                       "models": {"builder": "model"}, "budgets": {"run_tokens": 123}}

    def snapshot(value):
        kwargs = {"settings": public_settings, "execution": {"command": "run tests", "model": "model"},
                  "prompts": {"builder": "run tests", "verifier": "check model output"}}
        if source == "settings":
            kwargs["settings"] = {**public_settings,
                                  "qa": {**public_settings["qa"], "accessToken": value},
                                  "build": {**public_settings["build"], "access-token": value}}
        elif source == "execution":
            kwargs["execution"] = {**kwargs["execution"], "apiKey": value,
                                   "env": {"CUSTOM_CREDENTIAL": value}}
        elif source == "env":
            monkeypatch.setenv("EVAL_API_KEY", value)
        else:
            kwargs["credential_values"] = (value,)
        return configuration(ws, **kwargs)

    first = snapshot(secret)
    second = snapshot("rotated-credential-value")
    assert first.id == second.id
    assert first.snapshot == second.snapshot
    assert first.snapshot["prompts"] == {"builder": "run tests", "verifier": "check model output"}
    assert first.snapshot["execution"] == {"command": "run tests", "model": "model"}
    for branch in ("qa", "build"):
        assert first.snapshot["settings"][branch] == public_settings[branch]
    assert first.snapshot["settings"]["models"]["builder"] == "model"
    assert first.snapshot["settings"]["budgets"]["run_tokens"] == 123
    assert "rotated-credential-value" not in json.dumps(second.snapshot)


@pytest.mark.parametrize("text", ["https://user:pass@example.com", "https://example.com?token=secret",
                                 "https://example.com?api_key=secret", "Bearer secret"])
def test_credential_bearing_public_text_is_refused(text):
    ws = workspace()
    for kwargs in ({"execution": {"url": text}}, {"prompts": {"builder": text}},
                   {"settings": {"qa": {"start": text}}}):
        with pytest.raises(b.SuiteError, match="Credential-bearing execution text"):
            configuration(ws, **kwargs)
    with db.session_scope() as s:
        assert s.query(EvalConfiguration).count() == 0


@pytest.mark.parametrize("key", ["access_token", "password", "api-key", "accessToken", "ACCESS_TOKEN",
                                "apiKey", "api_key", "API_KEY", "feedback-key", "clientSecret",
                                "authorization", "provider_api_key", "access%5Ftoken", "%70assword",
                                "api%2Dkey"])
@pytest.mark.parametrize("template", ["https://example.com?{}=secret-query-value",
                                    "curl 'https://example.com?public=ok&{}=secret-query-value'",
                                    "https://example.com/callback#{}=secret-query-value",
                                    "curl 'https://example.com/callback#public=ok&{}=secret-query-value'"])
def test_normalized_credential_query_parameters_are_refused(key, template):
    ws = workspace()
    text = template.format(key)
    for kwargs in ({"execution": {"url": text}}, {"prompts": {"builder": text}},
                   {"settings": {"qa": {"start": text}, "build": {"start": text}}}):
        with pytest.raises(b.SuiteError, match="Credential-bearing execution text"):
            configuration(ws, **kwargs)
    with db.session_scope() as s:
        assert s.query(EvalConfiguration).count() == 0


def test_public_query_parameters_are_preserved_and_versioned():
    ws = workspace()
    text = "https://example.com?sort_key=name&run_tokens=123&max_tokens=456"
    cfg = configuration(ws, execution={"url": text}, prompts={"builder": text})
    assert cfg.snapshot["execution"]["url"] == text
    assert cfg.snapshot["prompts"]["builder"] == text
    assert cfg.id != configuration(ws, execution={"url": text + "7"}, prompts={"builder": text}).id


@pytest.mark.parametrize("key", ["token_budget", "max_input_tokens", "maxInputTokens", "max-input-tokens",
                                "max_completion_tokens", "context_token_limit", "total_tokens",
                                "per_request_token_ceiling", "daily_token_budget", "token_count",
                                "input_token_count", "tokens_per_hour", "tokenCount", "input-token-count",
                                "TOKENS_PER_HOUR", "hourly_token_count"])
@pytest.mark.parametrize("branch", ["limits", "budgets", "qa", "build"])
def test_behavioral_token_limits_are_preserved_and_versioned(branch, key):
    ws = workspace()
    first = configuration(ws, settings={branch: {key: 123}})
    second = configuration(ws, settings={branch: {key: 456}})
    assert first.snapshot["settings"][branch][key] == 123
    assert second.snapshot["settings"][branch][key] == 456
    assert first.id != second.id
    text = f"https://example.com?{key}=123#{key}=456"
    cfg = configuration(ws, execution={key: 123, "url": text})
    assert cfg.snapshot["execution"] == {key: 123, "url": text}


@pytest.mark.parametrize("key", ["access_token_budget", "max_auth_token", "max_access_token",
                                "input_token_secret", "max_api_token", "refresh_token_limit",
                                "max_tokens_password", "token", "tokens_secret"])
def test_token_limits_do_not_exempt_credential_fields(key):
    ws = workspace()
    first = configuration(ws, settings={"qa": {key: "first-secret"}, "build": {key: "first-secret"}})
    second = configuration(ws, settings={"qa": {key: "rotated-secret"}, "build": {key: "rotated-secret"}})
    assert first.id == second.id
    for branch in ("qa", "build"):
        assert key not in first.snapshot["settings"][branch]
    with pytest.raises(b.SuiteError, match="Credential-bearing execution text"):
        configuration(ws, execution={"url": f"https://example.com#{key}=secret"})


@pytest.mark.parametrize("key", ["pat", "github_pat", "githubPat", "github-pat", "GITHUB_PAT",
                                "auth", "authentication", "github_auth", "providerAuthentication"])
def test_pat_and_auth_credentials_never_reach_snapshot_or_hash(key):
    ws = workspace()

    def snapshot(secret):
        return configuration(ws, settings={key: secret, "qa": {key: secret}, "build": {key: secret}},
                             execution={key: secret, "sandbox_kind": "vm"})

    first = snapshot("first-pat-value")
    second = snapshot("rotated-pat-value")
    assert first.id == second.id
    assert first.snapshot == second.snapshot
    assert key not in first.snapshot["settings"]
    for branch in ("qa", "build"):
        assert key not in first.snapshot["settings"][branch]
    assert first.snapshot["execution"] == {"sandbox_kind": "vm"}
    assert "first-pat-value" not in json.dumps(first.snapshot)
    assert "rotated-pat-value" not in json.dumps(second.snapshot)
    for text in (f"https://example.com?{key}=embedded-pat", f"https://example.com#{key}=embedded-pat"):
        with pytest.raises(b.SuiteError, match="Credential-bearing execution text"):
            configuration(ws, prompts={"builder": text})
