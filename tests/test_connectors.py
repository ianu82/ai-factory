from __future__ import annotations

import json

import pytest

from auto_mindsdb_factory.connectors import (
    FactoryConnectorError,
    FileBackedOpsConnector,
)


def test_file_backed_ops_connector_seeds_and_reads_default_signals(tmp_path) -> None:
    connector = FileBackedOpsConnector(tmp_path / "store")

    connector.ensure_default_signals("work-123")

    assert connector.read_rollback_signal("work-123")["status"] == "passed"
    assert connector.read_staging_signal("work-123")["soak_minutes"] == 1440
    assert connector.read_monitoring_signal("work-123")["window_minutes"] == 240


def test_file_backed_ops_connector_requires_existing_signals_when_not_seeded(tmp_path) -> None:
    connector = FileBackedOpsConnector(tmp_path / "store", seed_missing_signals=False)

    with pytest.raises(FactoryConnectorError, match="Missing staging signal"):
        connector.read_staging_signal("work-123")


def test_file_backed_ops_connector_rejects_failed_rollback_probe(tmp_path) -> None:
    connector = FileBackedOpsConnector(tmp_path / "store", seed_missing_signals=False)
    signal_path = tmp_path / "store" / "ops-signals" / "work-123" / "rollback-signal.json"
    signal_path.parent.mkdir(parents=True)
    signal_path.write_text(
        json.dumps(
            {
                "work_item_id": "work-123",
                "tested": True,
                "status": "failed",
                "evidence": "rollback probe failed",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(FactoryConnectorError, match="status must be 'passed'"):
        connector.read_rollback_signal("work-123")
