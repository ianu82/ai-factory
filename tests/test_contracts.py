from __future__ import annotations

from pathlib import Path

from auto_mindsdb_factory.contracts import validate_repository_contracts


def test_repository_contracts_validate() -> None:
    root = Path(__file__).resolve().parents[1]
    assert validate_repository_contracts(root) == []

