# Three-model Search

Search can share a small Lightning, medium Super and large hosted GLM pool. A task classifier chooses once using the question and the retrieved evidence; the selected model then streams the answer. Focused lookups use the small tier, comparisons and synthesis use the medium tier, and unusually demanding reasoning can use the large tier when explicitly permitted. These are routing recommendations, not claims that model size guarantees answer quality.

The Search page has an unchecked **Allow remote inference** option. Enabling it permits this query and its retrieved excerpts to reach the configured hosted model. The choice resets after the query and is independent of web search. Search never starts Deep research automatically. With no collections selected, model-only answers use the same routing policy.

The complete answer input is retained. Hosted eligibility uses a conservative UTF-8 byte bound plus chat-framing/output allowances against the configured remote context budget; this is not an exact tokenizer measurement. Inputs outside that budget stay local with an explicit warning. Model context rejection remains an error; inputs are never silently truncated.

The response reports selected tier/model, policy identity, route and response-header latency. The header latency includes classification and upstream startup; it is not isolated classifier inference time. Normal time-to-first-content and completion timings remain available. The client checks the router's selected-model header before releasing text and rejects a changed identity in streamed events. It does not retry a failed visible answer on another model.

## Configure and qualify

Render configuration offline using the actual model IDs and qualified context windows:

```bash
python3 deployment/routing/render.py \
  --efficient-endpoint http://lightning.MODEL_NAMESPACE.svc.cluster.local:8000/v1 \
  --capable-endpoint http://super.MODEL_NAMESPACE.svc.cluster.local:8000/v1 \
  --efficient-model nvidia/nemotron-3.5-lightning \
  --capable-model nvidia/nemotron-3.5-super-vl-120b-a12b \
  --router-endpoint http://rag-switchyard.APP_NAMESPACE.svc.cluster.local:4000/v1 \
  --hosted-endpoint https://inference-api.nvidia.com/v1 \
  --hosted-model nvidia/zai-org/glm-5.2 \
  --context-window 262144 --hosted-context-window 131072 \
  --output local/routing
```

Omit both hosted arguments for a local-only installation. The configured windows must be qualified against the actual serving profiles; they do not enlarge a model's capacity. Search has separate local-only and hosted-enabled routes. Its Super classifier uses a dedicated target/client with `enable_thinking=false`, preventing hidden reasoning from consuming the short routing-verdict budget while preserving the other workload policies.

Validate the rendered policy with the qualified Switchyard v0.3.0 image's `--dry-run`. Exercise all three tiers, per-query consent, complete-context rejection, source grounding, streaming, time to first content, concurrency, failure cleanup and native Run:ai lifecycle before activation. `deployment/routing/qualify_search_fixture.py` uses synthetic CPU endpoints and the real router binary. `qualify_search_live.py` runs bounded synthetic prompts against actual models; hosted testing requires explicit authorization and `NVIDIA_REMOTE_API_KEY` in the operator environment. Neither test loads source collections or records prompt text in its receipts.

Create a versioned ConfigMap containing `routes.toml`, `research-client.json` and `search-client.json`. Provision the provider key separately as a namespace-local Secret. Never put its value in Helm values, commands, Git or client config.

```yaml
routing:
  enabled: true
  searchEnabled: true
  researchEnabled: false
  image: REGISTRY/SWITCHYARD_IMAGE@sha256:QUALIFIED_DIGEST
  configMap: ROUTING_CONFIGMAP
  hosted:
    enabled: true
    secretName: HOSTED_PROVIDER_SECRET
    secretKey: NVIDIA_REMOTE_API_KEY
```

Use the registered project's Run:ai AI Application/Helm release flow in [the runbook](../RUNAI_RUNBOOK.md). Hosted-only credentials reach the CPU router; the API gets the read-only Search client configuration. Neither receives extra GPU allocations. The Helm settings default off; this document is a deployment guide, not evidence that an installation is active or qualified.

For native workspaces, `deployment/routing/native.py --hosted-secret HOSTED_PROVIDER_SECRET` prepares the same secret reference and configuration without submitting resources. Enable `SEARCH_ROUTING_ENABLED=true` only on the API and mount `search-client.json` at `SEARCH_ROUTING_CONFIG_FILE`. The research worker keeps its independent policy and enable flag.

## Recovery

Disable `routing.searchEnabled` (or `SEARCH_ROUTING_ENABLED`) to return ordinary Search to its configured fixed model. Keep remote permission false until the hosted provider is qualified. Restore the previous immutable API image and versioned configuration if needed. Preserve model workloads, source records, VDB indexes, caches and in-flight research checkpoints. Do not replace a policy used by a checkpointed research job as an incidental Search rollback.
