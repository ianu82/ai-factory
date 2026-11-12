"""Workspace-scoped connectors: shared persistence, safe intake and token providers.

Connector-specific adapters and management endpoints build on this foundation.
"""

from .core import ConnectorError, claim_delivery, connector_view, ingest_signal, store_credential, validate_scope
from .providers import TokenResolution, resolve_token

__all__ = ["ConnectorError", "TokenResolution", "claim_delivery", "connector_view", "ingest_signal",
           "resolve_token", "store_credential", "validate_scope"]
