"""Isolated, non-delivering trials using the normal candidate gates."""
from __future__ import annotations

import asyncio
import json
import shutil
from collections import defaultdict
from statistics import median

from sqlalchemy import text

from ..benchmarks import SuiteError
from ..db import session_scope, utcnow
from ..gitops import remote_url
from ..models import BenchmarkSuite, BenchmarkTrial, Event, Outcome, Plan, Repository, Run, Task, Workspace
from .delivery import TaskRunner
from .runtime import workspace_settings

NETWORK_WARNING = ("Network isolation: unrestricted. Results are not proof against an agent that finds "
                   "the upstream repository by other means. Contaminated trials are excluded.")


def summarize(trials):
    groups = defaultdict(list)
    for trial in trials:
        if trial.status == "contaminated":
            continue
        groups[(trial.harness, trial.model, trial.config_version or "unknown",
                trial.configuration.get("sandbox_kind", "unknown"))].append(trial)
    result = []
    for (harness, model, version, sandbox), rows in sorted(groups.items()):
        costs = [r.cost_usd for r in rows if r.cost_known]
        durations = [(r.finished_at - r.started_at).total_seconds() for r in rows
                     if r.finished_at and r.started_at]
        result.append(dict(harness=harness, model=model, config_version=version, sandbox_kind=sandbox,
                           passed=sum(r.passed is True for r in rows), total=len(rows),
                           pass_rate=sum(r.passed is True for r in rows) / len(rows),
                           median_cost=median(costs) if costs else None, unknown_costs=len(rows)-len(costs),
                           median_duration=median(durations) if durations else None))
    return result


def print_summary(trials):
    print(NETWORK_WARNING)
    for row in summarize(trials):
        cost = f"${row['median_cost']:.4f} (known costs only)" if row['median_cost'] is not None else "unknown"
        duration = f"{row['median_duration']:.2f}s" if row['median_duration'] is not None else "unknown"
        print(f"{row['harness']} {row['model']} {row['config_version']} ({row['sandbox_kind']}): "
              f"{row['passed']}/{row['total']} passed ({row['pass_rate']:.1%}); median cost {cost}; "
              f"unknown costs {row['unknown_costs']}; median duration {duration}")


async def run_suite(workspace_id: str, suite_name: str, harness: str, model: str, trials: int):
    from ..harness import get_harness
    get_harness(harness)
    if not 1 <= trials <= 1000 or not model.strip():
        raise SuiteError("Trials must be between 1 and 1000 and model must be nonempty")
    settings = workspace_settings(workspace_id)
    settings["harness"] = {**settings["harness"], "builder": harness, "verifier": harness}
    settings["models"] = {**settings["models"], "builder": model, "verifier": model}
    with session_scope() as s:
        suite = (s.query(BenchmarkSuite).filter_by(workspace_id=workspace_id, name=suite_name)
                 .order_by(BenchmarkSuite.revision.desc()).first())
        if suite is None:
            raise SuiteError("Benchmark suite not loaded in this workspace")
        suite_id, revision, tasks = suite.id, suite.revision, json.loads(json.dumps(suite.tasks))
        repos = {r.full_name: (r.id, r.config) for r in s.query(Repository).filter_by(workspace_id=workspace_id)}
        if any(t["repository"] not in repos for t in tasks):
            raise SuiteError("Every suite repository must be connected to the workspace")
    completed = []
    locks = defaultdict(asyncio.Lock)
    for entry in tasks:
        for number in range(1, trials + 1):
            repo_id, config = repos[entry["repository"]]
            with session_scope() as s:
                if s.get_bind().dialect.name == "sqlite":
                    s.execute(text("BEGIN IMMEDIATE"))
                ws = s.query(Workspace).filter_by(id=workspace_id).with_for_update().one()
                ws.outcome_seq += 1
                outcome = Outcome(workspace_id=workspace_id, number=ws.outcome_seq, source="benchmark",
                                  status="benchmark", title=entry["title"], brief=entry["specification"], repo_ids=[repo_id])
                s.add(outcome)
                s.flush()
                plan = Plan(outcome_id=outcome.id, status="benchmark", content={"suite_id": suite_id})
                s.add(plan)
                s.flush()
                task = Task(plan_id=plan.id,workspace_id=workspace_id, outcome_id=outcome.id, repo_id=repo_id,
                            key=entry["key"], title=entry["title"], spec=entry["specification"],
                            acceptance=entry["acceptance"], base_sha=entry["base_commit"],
                            harness=harness, verifier_harness=harness, status="benchmark_pending")
                s.add(task)
                s.flush()
                trial = BenchmarkTrial(workspace_id=workspace_id, suite_id=suite_id, suite_revision=revision,
                                       task_key=entry["key"], trial_number=number, harness=harness, model=model,
                                       task_id=task.id, outcome_id=outcome.id, status="running", started_at=utcnow(),
                                       configuration={"settings": settings, "repository": config,
                                                      "network_isolation": "unrestricted"})
                s.add(trial)
                s.flush()
                trial_id, task_id = trial.id, task.id
            runner = None
            error = ""
            cancelled = False
            try:
                runner = TaskRunner(task_id, asyncio.Event(), lambda rid: locks[rid], benchmark_trial=trial)
                await runner.run()
            except asyncio.CancelledError:
                error, cancelled = "Cancelled", True
                raise
            except Exception as exc:
                error = str(exc) or type(exc).__name__
            finally:
                if runner and runner._benchmark_root:
                    shutil.rmtree(runner._benchmark_root, ignore_errors=True)
                with session_scope() as s:
                    row, task = s.get(BenchmarkTrial, trial_id), s.get(Task, task_id)
                    runs = s.query(Run).filter_by(benchmark_trial_id=trial_id).all()
                    builders = [r for r in runs if r.role == "builder"]
                    attempts = (task.evidence or {}).get("attempts") or []
                    passed = bool(attempts and attempts[-1]["result"] == "verified") and not error
                    needles = [remote_url(entry["repository"]).lower(),
                               f"github.com/{entry['repository']}".lower(),
                               f"api.github.com/repos/{entry['repository']}".lower()]
                    contaminated = any(any(n in (e.message + json.dumps(e.data)).lower() for n in needles)
                                       for e in s.query(Event).filter(Event.run_id.in_([r.id for r in runs]),
                                                                      Event.kind.in_(["agent_command", "agent_tool", "benchmark_command"])))
                    if contaminated:
                        row.status = "contaminated"
                    elif cancelled or task.status == "cancelled":
                        row.status = "cancelled"
                    else:
                        row.status = "completed" if passed else "failed"
                    row.passed = passed and row.status == "completed"
                    row.finished_at = utcnow()
                    row.checks = {"results": runner.checks if runner else [], "error": error or (task.evidence or {}).get("benchmark_error", "")}
                    row.verifier = runner.verifier_report if runner else {}
                    row.cost_known = bool(runs) and all(r.cost_known for r in runs)
                    row.cost_usd = sum(r.cost_usd for r in runs) if row.cost_known else None
                    if builders:
                        primary = builders[0]
                        row.config_version = primary.config_version
                        row.configuration = {**row.configuration, "primary_run_id": primary.id,
                                             "sandbox_kind": (primary.config_snapshot or {}).get("sandbox", "unknown")}
                        primary.result = {**primary.result, "benchmark": {"passed": row.passed,
                                          "checks": row.checks, "verifier": row.verifier}}
                        primary.status = "cancelled" if row.status == "cancelled" else "succeeded" if row.passed else "failed"
                    task.status, task.stage, task.finished_at = "benchmark_done", "done", row.finished_at
                    completed.append(row)
    return completed
