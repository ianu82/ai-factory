# MindsHub inference configuration

The optional trusted profile `inference` object records configuration metadata.
It does not activate a provider, resolve a key or qualify a gateway. Existing
profiles without it retain their canonical hashes, models and execution behavior.

Version 1 requires `provider: mindshub`, `protocol: responses` and the production
client endpoint `https://api.mindshub.ai/v1/responses`. It also requires a canonical
UUID `workspace_id` and a `credential_ref` with that same workspace ID, an opaque
UUID `id` and a positive integer `revision` (at most 2147483647). These identify
Factory records; membership, record issuance and secret resolution are separate
work. A reference neither contains a key nor grants access. Raw keys, other
fields and credential-bearing URLs are rejected without echoing their values.

`product-validate` reports the safe metadata, its digest, `status: configured`
and `execution_ready: false`. Explicit policies currently stop delivery, replay,
automatic intake and direct role initialization before credential resolution,
model requests or runner allocation. They cannot fall back to global OpenAI
credentials. The legacy provider path is unchanged when the object is absent.

Next connect approved workspace-scoped secret resolution, freeze the effective
provider/reference identity and qualify actual models, grammar, tools, usage and
cancellation for each supported harness. The endpoint is based on the existing
[pinned MindsHub client](https://github.com/mindsdb/cowork-server/blob/1a82f05cc49bbd1ed8401d357ae155c041b28d9f/cowork/coding/inference_proxy.py);
it is not live compatibility evidence. Keep Factory admission/retry/deadline
policy rather than importing Cowork's retry behavior. Keys stay outside jobs,
CLI child environments, logs and evidence. No deployment or activation occurs
when a profile is validated.
