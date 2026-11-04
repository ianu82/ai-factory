import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from auto_mindsdb_factory import product_commands
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.__main__ import _load_local_env_files, main
from auto_mindsdb_factory.run_journal import RunJournal


@pytest.fixture
def validation_checkout():
    # Keep Git metadata out of the exported source tree, even with --basetemp.
    with tempfile.TemporaryDirectory(prefix="product-validate-", dir="/tmp") as directory:
        root = Path(directory)
        target = root / "target"
        subprocess.run(["git", "init", "-q", str(target)], check=True)
        subprocess.run(["git", "-C", str(target), "remote", "add", "origin",
                        "git@github.com:owner/product.git"], check=True)
        yield root, target


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
    }


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
    assert json.loads(capsys.readouterr().out)["results"][0]["status"] == "complete"


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
    assert json.loads(capsys.readouterr().out)["results"][0]["status"] == "complete"


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
