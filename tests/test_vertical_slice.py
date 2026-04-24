from __future__ import annotations

import json
from pathlib import Path

import pytest

from auto_mindsdb_factory.connectors import (
    CommandEvidence,
    EvalEvidence,
    FactoryConnectorError,
    PullRequestEvidence,
    PullRequestStatus,
)
from auto_mindsdb_factory.vertical_slice import (
    FactoryVerticalSliceRunner,
    VerticalSliceConfig,
    build_cockpit_summary,
)


class FakeRepoConnector:
    def __init__(self, *, fail_create: bool = False) -> None:
        self.fail_create = fail_create

    def create_pull_request(self, *, work_item_id, spec_packet, ticket_bundle, pr_packet):
        if self.fail_create:
            raise FactoryConnectorError("missing PR evidence")
        return PullRequestEvidence(
            repository=pr_packet["pull_request"]["repository"],
            branch_name=f"factory/test-{work_item_id[-8:]}",
            base_branch="main",
            commit_sha="abc1234",
            number=42,
            url="https://github.com/ianu82/ai-factory/pull/42",
            title=pr_packet["pull_request"]["title"],
        )

    def read_pull_request_status(self, evidence):
        return PullRequestStatus(
            repository=evidence.repository,
            number=evidence.number,
            state="OPEN",
            mergeable="MERGEABLE",
            url=evidence.url,
            checks=[
                {
                    "name": "local-contracts",
                    "status": "success",
                    "url": None,
                }
            ],
        )


class FakeEvalConnector:
    def __init__(self, *, passed: bool = True) -> None:
        self.passed = passed

    def run_required_evals(self):
        status = "passed" if self.passed else "failed"
        exit_code = 0 if self.passed else 1
        return EvalEvidence(
            status=status,
            commands=[
                CommandEvidence(
                    command=["fake-eval"],
                    exit_code=exit_code,
                    stdout=status,
                    stderr="" if self.passed else "failed",
                )
            ],
        )


class UnhealthyOpsConnector:
    def ensure_default_signals(self, work_item_id: str) -> None:
        return None

    def read_rollback_signal(self, work_item_id: str):
        return {
            "work_item_id": work_item_id,
            "tested": True,
            "executed": False,
            "status": "passed",
            "evidence": "rollback probe passed",
        }

    def read_staging_signal(self, work_item_id: str):
        return {
            "work_item_id": work_item_id,
            "soak_minutes": 1440,
            "request_samples": 5000,
            "metrics": {},
        }

    def read_monitoring_signal(self, work_item_id: str):
        return {
            "work_item_id": work_item_id,
            "window_minutes": 45,
            "metrics": {"error_rate_pct": 99},
            "security_anomaly": False,
        }


def _config(tmp_path: Path) -> VerticalSliceConfig:
    root = Path(__file__).resolve().parents[1]
    return VerticalSliceConfig(
        repo_root=root,
        store_dir=tmp_path / "factory-store",
        repository="ianu82/ai-factory",
        html_file=root / "fixtures" / "intake" / "anthropic-release-notes-sample.html",
        entry_index=0,
    )


def _run_with_fakes(tmp_path: Path, **kwargs):
    return FactoryVerticalSliceRunner(
        _config(tmp_path),
        repo_connector=kwargs.get("repo_connector", FakeRepoConnector()),
        eval_connector=kwargs.get("eval_connector", FakeEvalConnector()),
        ops_connector=kwargs.get("ops_connector"),
    ).run()


def test_vertical_slice_reaches_stage9_with_pr_and_eval_evidence(tmp_path) -> None:
    result = _run_with_fakes(tmp_path)

    assert result.final_state == "PRODUCTION_MONITORING"
    assert result.pr_evidence.url == "https://github.com/ianu82/ai-factory/pull/42"
    assert result.eval_evidence.status == "passed"
    assert Path(result.stored_paths["stage9"]).exists()
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    stage3 = json.loads(Path(result.stored_paths["stage3"]).read_text(encoding="utf-8"))
    assert summary["feedback_report_id"] == result.feedback_report_id
    assert stage3["pr_packet"]["pull_request"]["number"] == 42


def test_vertical_slice_cockpit_summarizes_latest_run(tmp_path) -> None:
    result = _run_with_fakes(tmp_path)

    summary = build_cockpit_summary(tmp_path / "factory-store", repo_root_override=_config(tmp_path).repo_root)

    assert summary["run_count"] == 1
    assert summary["runs"][0]["work_item_id"] == result.work_item_id
    assert summary["runs"][0]["latest_stage"] == "stage9"
    assert summary["runs"][0]["pull_request"]["url"] == result.pr_evidence.url


def test_vertical_slice_fails_when_pr_evidence_is_missing(tmp_path) -> None:
    with pytest.raises(FactoryConnectorError, match="missing PR evidence"):
        _run_with_fakes(tmp_path, repo_connector=FakeRepoConnector(fail_create=True))


def test_vertical_slice_fails_when_required_evals_fail(tmp_path) -> None:
    with pytest.raises(FactoryConnectorError, match="Required local eval commands failed"):
        _run_with_fakes(tmp_path, eval_connector=FakeEvalConnector(passed=False))


def test_vertical_slice_records_unhealthy_monitoring_feedback(tmp_path) -> None:
    result = _run_with_fakes(tmp_path, ops_connector=UnhealthyOpsConnector())

    stage8 = json.loads(Path(result.stored_paths["stage8"]).read_text(encoding="utf-8"))
    stage9 = json.loads(Path(result.stored_paths["stage9"]).read_text(encoding="utf-8"))
    assert stage8["monitoring_report"]["monitoring_decision"]["status"] != "healthy"
    assert stage9["feedback_report"]["summary"]["incident_count"] >= 1
