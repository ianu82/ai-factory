"""Shared agent configuration for one-shot stages and the production worker."""

from __future__ import annotations

from typing import Any

from .connectors import (
    AgentConnector,
    FactoryConnectorError,
    OpenAIResponsesAgentConfig,
    OpenAIResponsesAgentConnector,
)


def build_agent_connector(provider: str, **overrides: Any) -> AgentConnector | None:
    if provider == "none":
        return None
    if provider != "openai":
        raise FactoryConnectorError(f"Unsupported agent provider: {provider}")
    try:
        config = OpenAIResponsesAgentConfig.from_env(**overrides)
    except ValueError as exc:
        raise FactoryConnectorError("OpenAI token and timeout settings must be integers.") from exc
    return OpenAIResponsesAgentConnector(config)
