import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from auto_mindsdb_factory import product_commands
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.__main__ import _load_local_env_files, main
from auto_mindsdb_factory.run_journal import RunJournal
from test_product_inference_runtime import inputs


def run_product_validate(profile, target):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    return subprocess.run(
        [sys.executable, "-m", "auto_mindsdb_factory", "product-validate",
         "--profile", str(profile), "--target-root", str(target)],
        cwd=target, env=env, capture_output=True, text=True,
    )


@pytest.mark.parametrize("overrides,builder,reviewer", [
    ({}, "gpt-6.1-terra", "gpt-6.1-terra"),
    ({"builder_model": "custom-builder"}, "custom-builder", "gpt-6.1-terra"),
    ({"reviewer_model": "custom-reviewer"}, "gpt-6.1-terra", "custom-reviewer"),
    ({"builder_model": "custom-builder", "reviewer_model": "custom-reviewer"},
     "custom-builder", "custom-reviewer"),
    ({"builder_harness": "native", "reviewer_harness": "native"},
     "gpt-6.1-terra", "gpt-6.1-terra"),
])
def test_product_validate_reports_effective_models(validation_checkout, overrides, builder, reviewer):
    root, target = validation_checkout
    document = {
        "version": 1, "name": "example", "repository": "Owner/Product",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["pytest"]},
        "smoke": [["true"]], **overrides,
    }
    profile_path = root / "profile.json"
    profile_path.write_text(json.dumps(document))
    expected_profile = ProductProfile.from_document(document)

    result = run_product_validate(profile_path, target)

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "status": "valid", "profile_sha": expected_profile.digest,
        "repository": "Owner/Product", "builder_model": builder, "reviewer_model": reviewer,
        "builder_harness": "native", "reviewer_harness": "native",
    }


@pytest.mark.parametrize("role", ["builder_harness", "reviewer_harness"])
@pytest.mark.parametrize("harness", ["codex", "typo"])
def test_product_validate_rejects_unavailable_harnesses(validation_checkout, role, harness):
    root, target = validation_checkout
    document = {
        "version": 1, "name": "example", "repository": "owner/product",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["pytest"]},
        "smoke": [["true"]], role: harness,
    }
    profile_path = root / "profile.json"
    profile_path.write_text(json.dumps(document))
    result = run_product_validate(profile_path, target)
    assert result.returncode == 1
    assert result.stdout == ""
    assert "Product workflow failed:" in result.stderr


def test_product_validate_still_checks_origin_and_external_profile(validation_checkout):
    root, target = validation_checkout
    document = {
        "version": 1, "name": "example", "repository": "owner/other",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["pytest"]},
        "smoke": [["true"]],
    }
    external_profile = root / "profile.json"
    external_profile.write_text(json.dumps(document))
    result = run_product_validate(external_profile, target)
    assert result.returncode == 1
    assert "origin does not match" in result.stderr

    document["repository"] = "owner/product"
    internal_profile = target / "profile.json"
    internal_profile.write_text(json.dumps(document))
    result = run_product_validate(internal_profile, target)
    assert result.returncode == 1
    assert "outside the candidate repository" in result.stderr


def test_product_commands_do_not_load_target_env_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("OPENAI_API_KEY=candidate-controlled\n")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert _load_local_env_files(["product-deliver"]) == []
    assert "OPENAI_API_KEY" not in os.environ


def test_inspect_reports_terminal_state_and_retains_events(tmp_path, capsys):
    journal = RunJournal(tmp_path, "task")
    journal.create({})
    journal.append("run_finished", status="blocked")
    assert main(["product-inspect", "--store-dir", str(tmp_path), "--run-id", "task", "--events"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "blocked"
    assert output["timeline"][-1]["kind"] == "run_finished"


def _tree_snapshot(root):
    # Capture directories as well as exact file bytes and modification times.
    return {
        str(path.relative_to(root)): (path.lstat().st_mode, path.lstat().st_mtime_ns,
                                      os.readlink(path) if path.is_symlink() else
                                      None if path.is_dir() else path.read_bytes())
        for path in [root, *sorted(root.rglob("*"))]
    }


def _inspect(store, run_id, with_events):
    args = ["product-inspect", "--store-dir", str(store), f"--run-id={run_id}"]
    return main(args + (["--events"] if with_events else []))


@pytest.mark.parametrize("with_events", [False, True])
@pytest.mark.parametrize("layout", ["missing-store", "missing-delivery", "missing-run", "run-is-file"])
def test_inspect_missing_paths_never_write(tmp_path, capsys, with_events, layout):
    store = tmp_path / "store"
    if layout != "missing-store":
        store.mkdir()
        (store / "unrelated").write_bytes(b"do not change")
    if layout in {"missing-run", "run-is-file"}:
        (store / "delivery" / "other").mkdir(parents=True)
        (store / "delivery" / "other" / "event-000001.json").write_bytes(b"evidence")
    if layout == "run-is-file":
        (store / "delivery" / "task").write_bytes(b"not a directory")
    before = _tree_snapshot(tmp_path)
    assert _inspect(store, "task", with_events) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "Run does not exist" in output.err
    assert _tree_snapshot(tmp_path) == before


@pytest.mark.parametrize("run_id", ["..", "../escape", "a/b", "/absolute", "", "-bad", "x" * 97])
@pytest.mark.parametrize("store_exists", [False, True])
def test_inspect_invalid_ids_do_not_touch_files(tmp_path, capsys, run_id, store_exists):
    store = tmp_path / "store"
    if store_exists:
        (store / "delivery" / "existing").mkdir(parents=True)
        (store / "delivery" / "existing" / "manifest.json").write_bytes(b"{}")
    before = _tree_snapshot(tmp_path)
    assert _inspect(store, run_id, True) == 1
    output = capsys.readouterr()
    assert output.out == "" and "Invalid run id" in output.err
    assert _tree_snapshot(tmp_path) == before


@pytest.mark.parametrize("with_events", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_inspect_existing_journal_is_unchanged(tmp_path, capsys, with_events, empty):
    journal = RunJournal(tmp_path / "store", "task")
    if not empty:
        journal.create({"task": "test"})
        from auto_mindsdb_factory.product_roles import RoleDefinitionError, stored_role_snapshots
        with pytest.raises(RoleDefinitionError, match="Missing"):
            stored_role_snapshots(json.loads((journal.root / "manifest.json").read_bytes()))
        journal.append("attempt_started")
        journal.append("model_response", usage={"total_tokens": 23}, cost_estimate_usd=0.25)
        journal.append("run_finished", status="complete", duration_seconds=5)
        # Inspection does not require or create a lock file.
        (journal.root / ".journal.lock").unlink()
    expected = journal.summary()
    if with_events:
        expected["timeline"] = journal.events()
    before = _tree_snapshot(tmp_path)
    assert _inspect(tmp_path / "store", "task", with_events) == 0
    output = capsys.readouterr()
    assert output.err == "" and json.loads(output.out) == expected
    assert expected["status"] == ("incomplete" if empty else "complete")
    assert _tree_snapshot(tmp_path) == before


@pytest.mark.parametrize("with_events", [False, True])
@pytest.mark.parametrize("corruption", ["bad-json", "bad-data", "bad-kind", "bad-status", "gap", "misnamed", "bad-manifest", "dangling-manifest"])
def test_inspect_corrupt_or_gapped_evidence_never_writes(tmp_path, capsys, with_events, corruption):
    journal = RunJournal(tmp_path / "store", "task")
    journal.create({})
    journal.append("first")
    journal.append("second")
    first = journal.root / "event-000001.json"
    if corruption == "bad-json":
        first.write_text("{")
    elif corruption == "bad-data":
        first.write_text(json.dumps({"sequence": 1, "kind": "first", "data": []}))
    elif corruption == "bad-kind":
        event = json.loads(first.read_text())
        event["kind"] = None
        first.write_text(json.dumps(event))
    elif corruption == "bad-status":
        event = json.loads(first.read_text())
        event["kind"] = "run_finished"
        first.write_text(json.dumps(event))
    elif corruption == "gap":
        first.unlink()
    elif corruption == "misnamed":
        (journal.root / "event-000002.json").unlink()
        first.rename(journal.root / "event-000002.json")
    elif corruption == "bad-manifest":
        (journal.root / "manifest.json").write_text("{")
    else:
        manifest = journal.root / "manifest.json"
        manifest.unlink()
        manifest.symlink_to(journal.root / "missing")
    before = _tree_snapshot(tmp_path)
    assert _inspect(tmp_path / "store", "task", with_events) == 1
    output = capsys.readouterr()
    assert output.out == "" and "Run " in output.err
    assert _tree_snapshot(tmp_path) == before


def test_real_inspect_cli_missing_run_has_no_side_effects(tmp_path):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    store = tmp_path / "missing-store"
    before = _tree_snapshot(tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "auto_mindsdb_factory", "product-inspect",
         "--store-dir", str(store), "--run-id", "task", "--events"],
        cwd=tmp_path, env=env, capture_output=True, text=True,
    )
    assert result.returncode != 0 and result.stdout == ""
    assert "Run does not exist" in result.stderr
    assert _tree_snapshot(tmp_path) == before


@pytest.fixture
def linear_cli(tmp_path, monkeypatch):
    target = tmp_path / "target"
    target.mkdir()
    profile = tmp_path / "profile.json"
    profile.write_text(json.dumps({
        "version": 1, "name": "example", "repository": "owner/product",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["pytest"]},
        "smoke": [["true"]], "auto_merge": True, "required_status_checks": ["validate", "sandbox"],
    }))
    route = tmp_path / "route.json"
    route.write_text(json.dumps({
        "version": 2, "team_id": "trusted-team", "queued_state_id": "ready",
        "required_label": "factory", "acceptance_criteria": [
            {"id": "test", "description": "tests", "command": ["pytest"]}],
    }))
    monkeypatch.setattr(ProductProfile, "validate_repository", lambda self, root: None)
    monkeypatch.delenv("LINEAR_API_KEY", raising=False)
    monkeypatch.delenv("LINEAR_TARGET_STATE_ID", raising=False)
    monkeypatch.delenv("LINEAR_GRAPHQL_URL", raising=False)
    calls = []

    class Client:
        def __init__(self, config):
            calls.append(("client", config))

    class Worker:
        def __init__(self, profile, target_root, store, route, client):
            calls.append(("worker", profile, target_root, store, route, client))

        def run_cycle(self, max_issues):
            calls.append(("cycle", max_issues))
            return [{"issue_id": "issue", "status": "complete"}]

    monkeypatch.setattr(product_commands, "LinearProductClient", Client)
    monkeypatch.setattr(product_commands, "LinearProductWorker", Worker)
    argv = ["product-linear-worker", "--profile", str(profile), "--route", str(route),
            "--target-root", str(target), "--store-dir", str(tmp_path / "store")]
    return argv, calls, target, profile, route


def test_linear_once_routes_trusted_config_and_credentials(linear_cli, tmp_path, monkeypatch, capsys):
    argv, calls, target, _, _ = linear_cli
    monkeypatch.chdir(target)
    (target / ".env").write_text("LINEAR_API_KEY=untrusted\n")
    env = tmp_path / "env"
    env.write_text("LINEAR_API_KEY=file-key\nLINEAR_TARGET_STATE_ID=evil\nLINEAR_GRAPHQL_URL=https://evil\n")
    assert main(argv + ["--env-file", str(env), "--once", "--max-issues", "3"]) == 0
    config = calls[0][1]
    assert (config.target_team_id, config.target_state_id, config.api_key) == ("trusted-team", "ready", "file-key")
    assert config.graphql_url != "https://evil"
    assert "LINEAR_TARGET_STATE_ID" not in os.environ and "LINEAR_GRAPHQL_URL" not in os.environ
    assert calls[1][2:4] == (target, tmp_path / "store")
    assert calls[1][4].document["required_label"] == "factory"
    assert calls[2:] == [("cycle", 3)]
    assert json.loads(capsys.readouterr().out) == {
        "runner": "docker", "results": [{"issue_id": "issue", "status": "complete"}],
    }


def test_linear_railway_runner_requires_explicit_selection(linear_cli, monkeypatch, capsys):
    from auto_mindsdb_factory.railway_runner import RailwayDelivery
    argv, calls, _, _, _ = linear_cli
    monkeypatch.setenv("LINEAR_API_KEY", "key")
    selected = []
    class Worker:
        def __init__(self, *args, **kwargs):
            selected.append(kwargs["delivery_factory"])
        def run_cycle(self, max_issues):
            return []
    monkeypatch.setattr(product_commands, "LinearProductWorker", Worker)
    assert main(argv + ["--runner", "railway", "--once"]) == 0
    assert selected == [RailwayDelivery]
    assert json.loads(capsys.readouterr().out) == {"runner": "railway", "results": []}


@pytest.mark.parametrize("runner_args,runner", [([], "docker"), (["--runner", "railway"], "railway")])
def test_linear_continuous_reports_each_cycle(linear_cli, monkeypatch, capsys, runner_args, runner):
    argv, calls, _, _, _ = linear_cli
    monkeypatch.setenv("LINEAR_API_KEY", "secret-key")
    cycle_results = iter([[{"issue_id": "first", "status": "complete"}],
                          [{"issue_id": "second", "status": "blocked"}]])
    sleeps = []

    class Worker:
        def __init__(self, *args, **kwargs):
            pass

        def run_cycle(self, max_issues):
            calls.append(("cycle", max_issues))
            return next(cycle_results)

    def sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 2:
            raise KeyboardInterrupt

    monkeypatch.setattr(product_commands, "LinearProductWorker", Worker)
    monkeypatch.setattr(product_commands.time, "sleep", sleep)
    assert main(argv + runner_args + ["--poll-seconds", "5", "--max-issues", "3"]) == 0
    assert calls[1:] == [("cycle", 3), ("cycle", 3)]
    assert sleeps == [5, 5]
    assert [json.loads(line) for line in capsys.readouterr().out.splitlines()] == [
        {"runner": runner, "results": [{"issue_id": "first", "status": "complete"}]},
        {"runner": runner, "results": [{"issue_id": "second", "status": "blocked"}]},
    ]


@pytest.mark.parametrize("status", ["blocked", "pending", "conflict"])
def test_linear_once_preserves_blocked_exit_code(linear_cli, monkeypatch, capsys, status):
    argv, _, _, _, _ = linear_cli
    monkeypatch.setenv("LINEAR_API_KEY", "secret-key")
    results = [{"issue_id": "issue", "status": status, "reason": "inspect evidence"}]

    class Worker:
        def __init__(self, *args, **kwargs):
            pass

        def run_cycle(self, max_issues):
            return results

    monkeypatch.setattr(product_commands, "LinearProductWorker", Worker)
    assert main(argv + ["--once"]) == 1
    assert json.loads(capsys.readouterr().out) == {"runner": "docker", "results": results}


def test_linear_process_credential_wins_and_target_env_never_loaded(linear_cli, tmp_path, monkeypatch):
    argv, calls, target, _, _ = linear_cli
    monkeypatch.chdir(target)
    (target / ".env").write_text("LINEAR_API_KEY=candidate\n")
    assert main(argv + ["--once"]) == 1
    assert not calls
    env = tmp_path / "env"
    env.write_text("LINEAR_API_KEY=file-key\n")
    monkeypatch.setenv("LINEAR_API_KEY", "process-key")
    monkeypatch.setenv("LINEAR_TARGET_STATE_ID", "process-override")
    monkeypatch.setenv("LINEAR_GRAPHQL_URL", "https://process-override")
    assert main(argv + ["--once", "--env-file", str(env)]) == 0
    assert calls[0][1].api_key == "process-key"
    assert calls[0][1].target_state_id == "ready"
    assert calls[0][1].graphql_url != "https://process-override"


@pytest.mark.parametrize("flag,value", [
    ("--poll-seconds", "4"), ("--poll-seconds", "3601"),
    ("--max-issues", "0"), ("--max-issues", "101"),
])
def test_linear_bounds_fail_before_side_effects(linear_cli, flag, value):
    argv, calls, _, _, _ = linear_cli
    with pytest.raises(SystemExit) as exc:
        main(argv + [flag, value])
    assert exc.value.code != 0
    assert not calls


@pytest.mark.parametrize("flag", ["--profile", "--route", "--env-file", "--store-dir"])
def test_linear_rejects_inside_target(linear_cli, flag, monkeypatch):
    argv, calls, target, profile, route = linear_cli
    monkeypatch.setenv("LINEAR_API_KEY", "key")
    path = target / "input"
    if flag == "--profile":
        path.write_text(profile.read_text())
    elif flag == "--route":
        path.write_text(route.read_text())
    elif flag == "--env-file":
        path.write_text("LINEAR_API_KEY=key\n")
    if flag == "--env-file":
        argv += [flag, str(path)]
    else:
        argv[argv.index(flag) + 1] = str(path)
    assert main(argv + ["--once"]) == 1
    assert not calls


def test_linear_requires_auto_merge(linear_cli, monkeypatch):
    argv, calls, _, profile, _ = linear_cli
    document = json.loads(profile.read_text())
    document["auto_merge"] = False
    profile.write_text(json.dumps(document))
    monkeypatch.setenv("LINEAR_API_KEY", "key")
    assert main(argv + ["--once"]) == 1
    assert not calls


def test_linear_blocked_and_errors_are_honest_and_sanitized(linear_cli, monkeypatch, capsys):
    argv, _, _, _, _ = linear_cli
    monkeypatch.setenv("LINEAR_API_KEY", "private-key")

    class Worker:
        def __init__(self, *args):
            pass

        def run_cycle(self, max_issues):
            return [{"issue_id": "issue", "status": "blocked"}]

    monkeypatch.setattr(product_commands, "LinearProductWorker", Worker)
    assert main(argv + ["--once"]) == 1
    assert json.loads(capsys.readouterr().out)["results"][0]["status"] == "blocked"

    def fail(self, max_issues):
        raise RuntimeError("private-key sensitive provider response")

    monkeypatch.setattr(Worker, "run_cycle", fail)
    assert main(argv + ["--once"]) == 1
    assert "private-key" not in capsys.readouterr().err


def test_linear_poller_sleeps_and_interrupts_cleanly(linear_cli, monkeypatch, capsys):
    argv, calls, _, _, _ = linear_cli
    monkeypatch.setenv("LINEAR_API_KEY", "key")
    delays = []

    def sleep(seconds):
        delays.append(seconds)
        raise KeyboardInterrupt

    monkeypatch.setattr(product_commands.time, "sleep", sleep)
    assert main(argv + ["--poll-seconds", "5"]) == 0
    assert calls[-1] == ("cycle", 1)
    assert delays == [5]
    assert json.loads(capsys.readouterr().out) == {
        "runner": "docker", "results": [{"issue_id": "issue", "status": "complete"}],
    }


def test_linear_key_is_not_imported_by_other_product_commands(linear_cli, tmp_path, monkeypatch):
    _, _, target, profile, _ = linear_cli
    env = tmp_path / "env"
    env.write_text("LINEAR_API_KEY=not-for-replay\n")
    monkeypatch.setattr(product_commands, "run_suite", lambda *args, **kwargs: {"total": 0, "correct": 0})
    assert main(["product-replay", "--profile", str(profile), "--target-root", str(target),
                 "--store-dir", str(tmp_path / "store"), "--suite", str(tmp_path / "suite"),
                 "--env-file", str(env)]) == 0
    assert "LINEAR_API_KEY" not in os.environ


def test_linear_store_cannot_contain_target(linear_cli, tmp_path, monkeypatch):
    argv, calls, _, _, _ = linear_cli
    monkeypatch.setenv("LINEAR_API_KEY", "key")
    argv[argv.index("--store-dir") + 1] = str(tmp_path)
    assert main(argv + ["--once"]) == 1
    assert not calls


@pytest.mark.parametrize("checks", [None, [], [" "], ["validate", "\t"]])
def test_linear_requires_named_ci_policy_before_side_effects(linear_cli, monkeypatch, checks):
    argv, calls, _, profile, _ = linear_cli
    document = json.loads(profile.read_text())
    if checks is None:
        document.pop("required_status_checks")
    else:
        document["required_status_checks"] = checks
    profile.write_text(json.dumps(document))
    monkeypatch.setenv("LINEAR_API_KEY", "key")
    assert main(argv + ["--once"]) == 1
    assert not calls


@pytest.fixture
def bound_delivery(validation_checkout, inputs):
    from auto_mindsdb_factory.product_inference_runtime import resolve_execution_context, MindsHubQualificationConfig
    root, target = validation_checkout
    policy = {"version": 1, "scope": "controller", "provider": "mindshub", "protocol": "responses",
        "endpoint": MindsHubQualificationConfig.base_url,
        "credential_ref": {"id": "00000000-0000-4000-8000-000000000001", "revision": 1}}
    profile = ProductProfile.from_document({"version": 1, "name": "bound", "repository": "owner/product",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["true"]}, "smoke": [["true"]],
        "builder_harness": "anton", "reviewer_harness": "anton", "inference": policy})
    _, options = inputs
    context = resolve_execution_context(profile.document, **{**options, "target_root": target})
    path, task = root / "profile.json", root / "task.json"
    path.write_text(profile.canonical_json)
    task.write_text(json.dumps({"id": "bound", "title": "Useful task", "goal": "Check parser",
        "acceptance_criteria": [{"id": "a", "description": "Parse input", "command": ["true"]}]}))
    args = ["product-deliver", "--profile", str(path), "--task", str(task), "--target-root", str(target),
        "--store-dir", str(root / "store"), "--runner", "railway", "--publish", "--base-sha", "a" * 40]
    return profile, context, args


@pytest.mark.parametrize("mode", ["bound", "drift", "missing", "plain-object"])
def test_normal_main_dispatches_only_exact_trusted_context(bound_delivery, monkeypatch, capsys, mode):
    from auto_mindsdb_factory import railway_runner
    profile, context, args = bound_delivery
    calls = []
    class Delivery:
        def __init__(self, supplied, target, store, *, inference_context):
            assert supplied.digest == profile.digest and inference_context is context
            calls.append("constructor")
        def run(self, task, *, promote, base_sha):
            assert callable(promote) and base_sha == "a" * 40
            assert self.publication_transport == "controller-github-v1"
            calls.append("normal-publication-callback")
            return {"status": "test-only"}
    monkeypatch.setattr(railway_runner, "RailwayDelivery", Delivery)
    if mode == "drift":
        path = Path(args[2]); document = json.loads(path.read_text()); document["name"] = "changed"
        path.write_text(json.dumps(document))
    supplied = {} if mode == "missing" else {"inference_context": {} if mode == "plain-object" else context}
    assert main(args, **supplied) == (0 if mode == "bound" else 1)
    assert calls == (["constructor", "normal-publication-callback"] if mode == "bound" else [])
    assert "mdb_synthetic.private" not in str(capsys.readouterr())


def test_context_cannot_admit_replay_linear_inspection_or_other_commands(bound_delivery, capsys):
    from types import SimpleNamespace
    _, context, _ = bound_delivery
    for command in ("product-replay", "product-linear-worker", "product-validate", "product-inspect"):
        assert product_commands.handle_product_command(SimpleNamespace(command=command),
            parse_env=lambda path: {}, inference_context=context) == 1
    assert main(["validate-contracts"], inference_context=context) == 1
    assert "supported only for product-deliver" in capsys.readouterr().err
