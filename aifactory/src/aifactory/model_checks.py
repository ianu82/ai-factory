"""Explicit, bounded image-input probes for saved workspace role selections.

Results are deliberately ephemeral. A failed check is not evidence that a model
cannot see images, and provider error text must never reach the API or logs.
"""

from __future__ import annotations

import asyncio
from typing import Any

from .image_input import rejects_image_input
from .inference import InferenceError, ModelClient, resolve_provider, workspace_models

# A valid 1 x 1 RGB PNG, embedded so checks need no filesystem or external URL.
TEST_IMAGE = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGNgYGAAAAAEAAH2FzhVAAAAAElFTkSuQmCC"

CHECK_TIMEOUT = 15.0
CHECK_TOKENS = 32
ROLES = ("planner", "plan_reviewer", "builder", "verifier")


async def check_workspace_models(workspace_id: str, settings: dict[str, Any]) -> dict[str, Any]:
    try:
        provider = resolve_provider(workspace_id)
    except Exception:
        selected = workspace_models(settings)
        return {"provider": None, "roles": {
            role: {"model": getattr(selected, role), "status": "unknown",
                   "explanation": "Check failed: model provider credentials are unavailable."}
            for role in ROLES
        }}
    # The same selection runtime makes: the resolved provider picks the plan reviewer's default.
    selected = workspace_models(settings, provider=provider.provider)
    effective = {role: provider.model_override or getattr(selected, role) for role in ROLES}
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
