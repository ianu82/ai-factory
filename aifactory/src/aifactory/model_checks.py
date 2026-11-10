"""Explicit, bounded image-input probes for saved workspace role selections.

Results are deliberately ephemeral. A failed check is not evidence that a model
cannot see images, and provider error text must never reach the API or logs.
"""

from __future__ import annotations

import asyncio
from typing import Any

from .inference import ModelClient, resolve_provider, workspace_models
from .images import is_image_rejection

# A valid 1 x 1 RGB PNG, embedded so checks need no filesystem or external URL.
TEST_IMAGE = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGNgYGAAAAAEAAH2FzhVAAAAAElFTkSuQmCC"

CHECK_TIMEOUT = 15.0
CHECK_TOKENS = 32
ROLES = ("planner", "builder", "verifier")


def rejects_image_input(exc: Exception) -> bool:
    """The same conservative capability rule used by execution harnesses."""
    return is_image_rejection(exc)


async def check_workspace_models(workspace_id: str, settings: dict[str, Any]) -> dict[str, Any]:
    selected = workspace_models(settings)
    effective = {role: getattr(selected, role) for role in ROLES}
    try:
        provider = resolve_provider(workspace_id)
    except Exception:
        return {"provider": None, "roles": {
            role: {"model": model, "status": "unknown",
                   "explanation": "Check failed: model provider credentials are unavailable."}
            for role, model in effective.items()
        }}
    effective = {role: provider.model_override or model for role, model in effective.items()}
    client = ModelClient(provider, timeout=CHECK_TIMEOUT, max_retries=0, log_errors=False)

    async def probe(model: str) -> dict[str, str]:
        try:
            # Use the same Responses endpoint as runtime. Never retry without the image.
            await asyncio.wait_for(client._post("/responses", {
                "model": model, "store": False, "max_output_tokens": CHECK_TOKENS,
                "include": ["reasoning.encrypted_content"],
                "input": [{"role": "user", "content": [
                    {"type": "input_text", "text": "Reply OK if you can see this image."},
                    {"type": "input_image", "image_url": TEST_IMAGE},
                ]}],
            }), timeout=CHECK_TIMEOUT)
            return {"model": model, "status": "supported", "explanation": "The model accepted image input."}
        except Exception as exc:
            if rejects_image_input(exc):
                return {"model": model, "status": "unsupported",
                        "explanation": "The provider explicitly rejected image input for this model."}
            return {"model": model, "status": "unknown",
                    "explanation": "Check failed: could not confirm image support. Check credentials and provider availability."}

    # One provider is resolved for the request; equal effective models share a probe.
    unique = list(dict.fromkeys(effective.values()))
    results = dict(zip(unique, await asyncio.gather(*(probe(model) for model in unique))))
    return {"provider": provider.provider, "roles": {role: results[model] for role, model in effective.items()}}
