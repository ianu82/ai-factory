"""Suite validation, tenant isolation, offline history and upgrade regressions."""

import copy
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from sqlalchemy import inspect, text

from aifactory import db
from aifactory.benchmarks import SuiteError, default_output_root, export_history, load_suite, parse_suite
from aifactory.cli import main
from aifactory.db import session_scope
from aifactory.models import BenchmarkSuite, Organization, Outcome, Plan, Repository, Run, Task, Workspace


def seed(repo="example/app"):
    with session_scope() as session:
        org = Organization(name="test")
        session.add(org)
        session.flush()
        ws = Workspace(org_id=org.id, name="test", slug="test")
        session.add(ws)
        session.flush()
        repository = Repository(workspace_id=ws.id, full_name=repo)
        session.add(repository)
        session.flush()
        outcome = Outcome(workspace_id=ws.id, number=1, title="Delivered")
        session.add(outcome)
        session.flush()
        plan = Plan(outcome_id=outcome.id)
        session.add(plan)
        session.flush()
        task = Task(workspace_id=ws.id, outcome_id=outcome.id, plan_id=plan.id, repo_id=repository.id,
                    key="T1", title="Preserve this", spec="  Exact spec\n\nwith whitespace.\n",
                    acceptance=["  exact criterion\n", "second criterion"], status="completed",
                    base_sha="a" * 40, merge_sha="b" * 40, pr_number=12,
                    pr_url=f"https://github.com/{repo}/pull/12")
        session.add(task)
        session.flush()
        return ws.id, repository.id, task.id


def document():
    return {"schema_version": 1, "name": "sample", "tasks": [{
        "key": "T1", "repository": "example/app", "base_commit": "a" * 40,
        "title": "A task", "specification": "Exact specification", "acceptance": ["It works"],
    }]}


def write(tmp_path, value):
    path = tmp_path / "suite.json"
    path.write_text(json.dumps(value))
    return path


def test_cli_load_revisions_and_workspace_scope(tmp_path, capsys):
    ws, _, _ = seed()
    other, _, _ = seed()
    path = write(tmp_path, document())
    main(["benchmarks", "load", "--workspace", ws, "--path", str(path)])
    assert "revision 1" in capsys.readouterr().out
    first = load_suite(ws, path)
    assert first.tasks == document()["tasks"]
    assert load_suite(ws, tmp_path).id == first.id
    changed = document()
    changed["tasks"][0]["specification"] = "Updated"
    second = load_suite(ws, write(tmp_path, changed))
    assert second.revision == 2 and second.content_hash != first.content_hash
    assert load_suite(ws, write(tmp_path, document())).id == first.id
    with session_scope() as session:
        assert session.query(BenchmarkSuite).filter_by(workspace_id=other).count() == 0
        assert session.query(BenchmarkSuite).filter_by(workspace_id=ws).count() == 2
        assert session.get(BenchmarkSuite, first.id).tasks == document()["tasks"]
    with pytest.raises(ValueError, match="immutable"), session_scope() as session:
        session.get(BenchmarkSuite, first.id).content_hash = "c" * 64


@pytest.mark.parametrize("arguments, expected", [
    (["--help"], "benchmarks"),
    (["benchmarks", "--help"], "import-history"),
    (["benchmarks", "load", "--help"], "--workspace"),
    (["benchmarks", "import-history", "--help"], "--output-root"),
])
def test_cli_help(arguments, expected, capsys):
    with pytest.raises(SystemExit) as exc:
        main(arguments)
    assert exc.value.code == 0
    assert expected in capsys.readouterr().out


@pytest.mark.parametrize("mutation", [
    lambda s: s.update(name="../escape"),
    lambda s: s.update(schema_version=True),
    lambda s: s.update(schema_version=2),
    lambda s: s.update(tasks=[]),
    lambda s: s["tasks"].append(copy.deepcopy(s["tasks"][0])),
    lambda s: s["tasks"][0].update(base_commit="main"),
    lambda s: s["tasks"][0].update(base_commit="deadbee"),
    lambda s: s["tasks"][0].update(acceptance=[]),
    lambda s: s["tasks"][0].update(acceptance=[""]),
    lambda s: s["tasks"][0].update(specification=" "),
    lambda s: s["tasks"][0].update(reference_change={"pr_number": 1}),
    lambda s: s["tasks"][0].update(reference_change={
        "pr_number": 1, "pr_url": "https://github.com/foreign/repo/pull/1", "merge_sha": "b" * 40}),
    lambda s: s["tasks"].append({**s["tasks"][0], "key": "T2", "repository": "foreign/repo"}),
])
def test_invalid_suite_is_atomic(tmp_path, mutation):
    ws, _, _ = seed()
    baseline = load_suite(ws, write(tmp_path, document()))
    invalid = document()
    mutation(invalid)
    with pytest.raises(SuiteError):
        load_suite(ws, write(tmp_path, invalid))
    with session_scope() as session:
        assert session.query(BenchmarkSuite).count() == 1
        assert session.get(BenchmarkSuite, baseline.id).tasks == document()["tasks"]


@pytest.mark.parametrize("raw", ['{broken', '{"name":"x","name":"y"}', 'null', '\xff'])
def test_malformed_json(tmp_path, raw):
    ws, _, _ = seed()
    path = tmp_path / "suite.json"
    path.write_bytes(raw.encode("latin1"))
    with pytest.raises(SuiteError):
        load_suite(ws, path)
    with session_scope() as session:
        assert session.query(BenchmarkSuite).count() == 0


def test_repository_only_in_another_workspace(tmp_path):
    ws, _, _ = seed("local/app")
    seed()
    with pytest.raises(SuiteError, match="not in workspace"):
        load_suite(ws, write(tmp_path, document()))
    with session_scope() as session:
        assert session.query(BenchmarkSuite).count() == 0


def test_history_export_is_offline_and_round_trips(tmp_path, monkeypatch, capsys):
    ws, repo_id, task_id = seed()
    seed()  # Identical repository name in another workspace must not leak.
    with session_scope() as session:
        original = session.get(Task, task_id)
        for index, fields in enumerate([
            {"status": "running"}, {"base_sha": ""}, {"merge_sha": ""},
            {"pr_url": ""}, {"acceptance": []},
        ]):
            values = {
                "workspace_id": ws, "outcome_id": original.outcome_id, "plan_id": original.plan_id,
                "repo_id": repo_id, "key": f"bad{index}", "title": "incomplete", "spec": "spec",
                "acceptance": ["ok"], "status": "completed", "base_sha": "a" * 40,
                "merge_sha": "b" * 40, "pr_number": 12, "pr_url": "https://github.com/example/app/pull/12",
            }
            session.add(Task(**(values | fields)))

    def forbidden(*args, **kwargs):
        raise AssertionError("External access forbidden")

    import httpx
    import subprocess
    from aifactory import github, gitops, security, inference

    with monkeypatch.context() as offline:
        for module in (github, gitops, security, inference):
            for name in dir(module):
                obj = getattr(module, name)
                if callable(obj) and getattr(obj, "__module__", "") == module.__name__ and name != "reset_crypto_cache":
                    offline.setattr(module, name, forbidden)
        offline.setattr(httpx.Client, "request", forbidden)
        offline.setattr(httpx.AsyncClient, "request", forbidden)
        offline.setattr(subprocess, "run", forbidden)
        offline.setattr(subprocess, "Popen", forbidden)
        main(["benchmarks", "import-history", "--workspace", ws, "--repo", "example/app",
              "--suite", "history", "--output-root", str(tmp_path)])
        output = capsys.readouterr().out
        assert "Included 1 task(s); excluded 5 task(s)" in output
        for reason in ("not completed", "missing base_sha", "missing merge/PR evidence", "invalid stored record"):
            assert reason in output
        path = tmp_path / "history/suite.json"
        exported = parse_suite(path)["tasks"][0]
        assert exported["key"] == f"task-{task_id}"
        assert exported["specification"] == "  Exact spec\n\nwith whitespace.\n"
        assert exported["acceptance"] == ["  exact criterion\n", "second criterion"]
        assert exported["base_commit"] == "a" * 40
        assert exported["reference_change"] == {
            "pr_number": 12, "pr_url": "https://github.com/example/app/pull/12", "merge_sha": "b" * 40,
        }
        assert load_suite(ws, path).id == load_suite(ws, path).id


def test_empty_export_reports_and_does_not_write(tmp_path, capsys):
    ws, _, task_id = seed()
    with session_scope() as session:
        session.get(Task, task_id).base_sha = ""
    with pytest.raises(SuiteError, match="No complete"):
        export_history(ws, "example/app", "empty", tmp_path)
    assert "1: missing base_sha" in capsys.readouterr().out
    assert not (tmp_path / "empty").exists()


def test_default_export_root_uses_checkout_not_installed_module(tmp_path, monkeypatch):
    from aifactory import benchmarks

    monkeypatch.setattr(benchmarks, "__file__", "/usr/local/lib/python3.13/site-packages/aifactory/benchmarks.py")
    checkout = tmp_path / "checkout/aifactory"
    (checkout / "src/aifactory").mkdir(parents=True)
    (checkout / "src/aifactory/cli.py").touch()
    monkeypatch.chdir(checkout)
    assert default_output_root() == checkout / "benchmarks"
    ws, _, _ = seed()
    assert export_history(ws, "example/app", "default") == checkout / "benchmarks/default/suite.json"
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SuiteError, match="--output-root"):
        export_history(ws, "example/app", "no-checkout")
    assert export_history(ws, "example/app", "explicit", tmp_path).is_file()


@pytest.mark.parametrize("identical", [True, False])
def test_concurrent_loads_are_serialized(tmp_path, identical):
    ws, _, _ = seed()
    paths = []
    for index in range(6):
        directory = tmp_path / str(index)
        directory.mkdir()
        value = document()
        if not identical:
            value["tasks"][0]["specification"] += str(index)
        paths.append(write(directory, value))
    barrier = Barrier(len(paths))

    def load(path):
        barrier.wait(timeout=10)
        return load_suite(ws, path)

    with ThreadPoolExecutor(max_workers=len(paths)) as pool:
        results = list(pool.map(load, paths))
    assert len({r.id for r in results}) == (1 if identical else len(paths))
    assert sorted({r.revision for r in results}) == ([1] if identical else list(range(1, len(paths) + 1)))


def test_existing_runs_get_trial_index_and_supported_foreign_key():
    ws, _, task_id = seed()
    with session_scope() as session:
        task = session.get(Task, task_id)
        run = Run(workspace_id=ws, outcome_id=task.outcome_id, role="builder", harness="native")
        session.add(run)
        session.flush()
        run_id = run.id
    engine = db.engine()
    if engine.dialect.name == "sqlite":
        # Reconstruct the pre-feature schema (SQLite cannot DROP a FK column).
        from sqlalchemy import MetaData
        from sqlalchemy.schema import CreateTable

        old = Run.__table__.to_metadata(MetaData())
        old.constraints.difference_update(
            c for c in list(old.constraints)
            if any(col.name == "benchmark_trial_id" for col in c.columns)
        )
        old._columns.remove(old.c.benchmark_trial_id)
        # Compile against the complete metadata to resolve existing FK targets.
        old.metadata = Run.metadata
        columns = ", ".join(f'"{c.name}"' for c in old.columns)
        with engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
            connection.exec_driver_sql(f"CREATE TABLE runs_backup AS SELECT {columns} FROM runs")
            connection.exec_driver_sql("DROP TABLE runs")
            connection.execute(CreateTable(old))
            connection.exec_driver_sql(f"INSERT INTO runs ({columns}) SELECT {columns} FROM runs_backup")
            connection.exec_driver_sql("DROP TABLE runs_backup")
            connection.commit()
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
    else:
        with engine.begin() as connection:
            connection.execute(text("ALTER TABLE runs DROP CONSTRAINT fk_runs_benchmark_trial_id"))
            connection.execute(text("DROP INDEX ix_runs_benchmark_trial_id"))
            connection.execute(text("ALTER TABLE runs DROP COLUMN benchmark_trial_id"))
    db.init_engine()
    db.init_engine()  # Migration is idempotent.
    inspector = inspect(db.engine())
    assert "benchmark_trial_id" in {c["name"] for c in inspector.get_columns("runs")}
    assert "ix_runs_benchmark_trial_id" in {i["name"] for i in inspector.get_indexes("runs")}
    if engine.dialect.name == "postgresql":
        assert any(k["constrained_columns"] == ["benchmark_trial_id"]
                   for k in inspector.get_foreign_keys("runs"))
    with session_scope() as session:
        assert session.get(Run, run_id).benchmark_trial_id is None
