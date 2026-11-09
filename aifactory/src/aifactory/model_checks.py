"""Explicit, bounded image-input probes for saved workspace role selections.

Results are deliberately ephemeral. A failed check is not evidence that a model
cannot see images, and provider error text must never reach the API or logs.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

from .inference import InferenceError, ModelClient, resolve_provider, workspace_models

# A valid 1 x 1 RGB PNG, embedded so checks need no filesystem or external URL.
TEST_IMAGE = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGNgYGAAAAAEAAH2FzhVAAAAAElFTkSuQmCC"

CHECK_TIMEOUT = 15.0
CHECK_TOKENS = 32
ROLES = ("planner", "builder", "verifier")


def rejects_image_input(exc: Exception) -> bool:
    """Only an explicit capability rejection, not a representation error, is proof.

    Match a complete capability clause: an optional 'input' must not backtrack
    to 'image' and leave a format/type/size qualifier behind. A later sentence
    suggesting a different format does not undo an explicit capability rejection.
    """
    if not isinstance(exc, InferenceError) or exc.status not in (400, 422):
        return False
    image = r"(?:image(?:\s+input)?|images|vision|image_url)"
    # The boundary after the capability is intentionally stronger than \b.
    end = r"(?=\s*$)"
    capability = re.compile(
        rf"\b(?:does not|doesn't|do not|cannot|can't)\s+support\s+{image}{end}"
        rf"|\b{image}\s+(?:is|are)\s+not\s+supported(?:\s+(?:by|for)\s+(?:this|the)\s+model)?{end}",
        re.IGNORECASE,
    )
    representation = re.compile(
        r"\b(?:format|encoding|encoded|base64|data[\s-]+(?:urls?|uris?)|inline|"
        r"mime|type|size|large|resolution|dimensions|tiff|png|jpe?g|gif|webp)\b",
        re.IGNORECASE,
    )
    for clause in re.split(r"[.;!\n]", str(exc)):
        # A representation qualifier in this clause takes precedence, even if
        # its trailing words resemble a capability rejection (e.g. data URL
        # images are not supported). Separate advice in a later clause does not.
        if representation.search(clause):
            continue
        if capability.search(clause):
            return True
    return False


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
