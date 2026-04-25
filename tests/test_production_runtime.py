from __future__ import annotations

from auto_mindsdb_factory.production_runtime import FactoryDoctor


def test_factory_doctor_env_check_rejects_malformed_linear_ids(monkeypatch) -> None:
    monkeypatch.setenv(
        "LINEAR_TARGET_STATE_ID",
        "LINEAR_TARGET_STATE_ID=17fa210f-6809-46cf-8e51-6bfa69a25cfe",
    )

    check = FactoryDoctor._env_check("LINEAR_TARGET_STATE_ID")

    assert check == {
        "name": "env:LINEAR_TARGET_STATE_ID",
        "status": "failed",
        "summary": "must be a UUID",
    }


def test_factory_doctor_env_check_accepts_linear_uuid(monkeypatch) -> None:
    monkeypatch.setenv("LINEAR_TARGET_STATE_ID", "17fa210f-6809-46cf-8e51-6bfa69a25cfe")

    check = FactoryDoctor._env_check("LINEAR_TARGET_STATE_ID")

    assert check == {
        "name": "env:LINEAR_TARGET_STATE_ID",
        "status": "passed",
        "summary": "set",
    }
