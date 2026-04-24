from __future__ import annotations

import json

import pytest

from auto_mindsdb_factory.connectors import (
    AgentTask,
    FactoryConnectorError,
    FileBackedOpsConnector,
    OpenAIResponsesAgentConfig,
    OpenAIResponsesAgentConnector,
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


def test_openai_config_reads_defaults_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    config = OpenAIResponsesAgentConfig.from_env()

    assert config.api_key == "test-key"
    assert config.model == "gpt-5.4"
    assert config.reasoning_effort == "medium"
    assert config.max_output_tokens == 4000
    assert config.base_url == "https://api.openai.com/v1/responses"


def test_openai_connector_parses_structured_output() -> None:
    class FakeHTTPResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self.payload = payload

        def read(self) -> bytes:
            return json.dumps(self.payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    captured_request: dict[str, object] = {}

    def fake_urlopen(request, timeout):
        captured_request["url"] = request.full_url
        captured_request["timeout"] = timeout
        captured_request["body"] = json.loads(request.data.decode("utf-8"))
        return FakeHTTPResponse(
            {
                "id": "resp_123",
                "model": "gpt-5.4",
                "output_text": json.dumps(
                    {
                        "tickets": [
                            {
                                "slug": "contract",
                                "summary": "Draft the contract change.",
                            }
                        ]
                    }
                ),
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 25,
                    "total_tokens": 125,
                },
            }
        )

    connector = OpenAIResponsesAgentConnector(
        OpenAIResponsesAgentConfig(
            api_key="test-key",
            model="gpt-5.4",
            reasoning_effort="medium",
            max_output_tokens=512,
            timeout_seconds=30,
        ),
        urlopen_impl=fake_urlopen,
    )
    task = AgentTask(
        name="ticket-draft",
        instructions="Return one drafted ticket summary.",
        input_document={"title": "Test"},
        output_schema={
            "type": "object",
            "additionalProperties": False,
            "required": ["tickets"],
            "properties": {
                "tickets": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 1,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["slug", "summary"],
                        "properties": {
                            "slug": {"type": "string"},
                            "summary": {"type": "string"},
                        },
                    },
                }
            },
        },
    )

    result = connector.run_task(task)

    assert captured_request["url"] == "https://api.openai.com/v1/responses"
    assert captured_request["timeout"] == 30
    assert captured_request["body"]["model"] == "gpt-5.4"
    assert captured_request["body"]["reasoning"]["effort"] == "medium"
    assert captured_request["body"]["text"]["format"]["type"] == "json_schema"
    assert result.provider == "openai"
    assert result.model == "gpt-5.4"
    assert result.response_id == "resp_123"
    assert result.output_document["tickets"][0]["slug"] == "contract"


def test_openai_connector_requires_api_key() -> None:
    with pytest.raises(FactoryConnectorError, match="OPENAI_API_KEY"):
        OpenAIResponsesAgentConnector(
            OpenAIResponsesAgentConfig(api_key=None),
        )
