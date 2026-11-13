# Workload-specific model routing

Optional routing uses one CPU-only Switchyard service with different policies for research and disposable coding jobs. It is disabled by default. Model services, GPU allocation and model caches are managed independently.

| Caller | Policy | Lifetime |
|---|---|---|
| Interactive search | Existing fixed LLM | Request; native streaming |
| Deep research | Custom task classification | One persisted choice per planning or synthesis phase |
| PDF candidate selection | Custom task classification | Optional call site for the separate research-document tool integration |
| Stateless coding harness | Composite | One fresh session per disposable job, retained through its tool loop |
| Individual private document analysis | Existing fixed LLM | Existing private workflow |

The standalone coding route can be used by a compatible disposable coding harness. This release does not add a public `run_coding_task` API, launch a coding sandbox from research, or provide persistent per-user coding sessions. Sandbox creation, independent acceptance, artifact export and confirmed cleanup remain the trusted coding controller's responsibilities.

## Research behavior

The worker classifies planning and report synthesis independently. On the first successful response it checkpoints the selected approved model before consuming model text. Resumed phases use a fixed alias for that model, including after a router restart. Distinct job/phase identities prevent cross-job affinity. There remains an at-least-once window if a process fails after model execution but before the checkpoint commits.

Structured controller output is validated locally. Invalid JSON, duplicate entries, excess entries or unapproved selected IDs allow one explicitly recorded retry on the capable model when routing is enabled. This retry budget survives worker restart. Other failures use existing finite research-job retries. A report whose streaming has begun is never silently replaced with another model's answer.

Research checkpoints retain the routing policy hash, requested route, served model, retry reason, serving-completion usage when supplied, response-header latency, first-content latency for streams, and total call duration. They do not duplicate prompts or source text into the routing trace. Serving usage excludes the classifier call; use Switchyard's separate classifier statistics for total routing cost. Missing usage remains unknown. End-to-end call timing includes classifier time and checkpoint work, rather than claiming isolated GPU inference time.

Existing collection/MCP restrictions, provenance and the user's web-search choice are preserved. Selecting a stronger model cannot enable internet access, a hosted provider or additional tools. The renderer accepts local service/private endpoints only; cluster egress policy must enforce actual isolation. Routing headers provide affinity and telemetry, not authentication.

## Coding behavior and version contract

The supported configuration targets Switchyard **v0.3.0**, commit `336196f6fbfc97ddc71c1700f6092e564e9f23c2`. In this version Composite's classifier supplies a retained fallback tier; Stage Router can override that tier using tool history. This differs from newer upstream profile-selection behavior. Do not copy main-only settings into the release configuration.

Each coding controller job must send one fresh `x-switchyard-session-id` and retain it through every request and tool result. Supply complete tool history; the router does not execute tools. A process restart must preserve that identity and transcript. A fresh coding job must receive a new identity. Keep independent executable acceptance checks: classifier/stage scores do not establish correctness. The v0.3.0 Composite fallback on an unusable classifier verdict is the stage default, which may be efficient; include malformed-verdict behavior in qualification rather than treating classification as a quality/security gate.

No advisor, escalation-judge or verification gate buffers every generated report. No transcript truncation or automatic context compaction is introduced. Requests that exceed a configured model limit fail explicitly; inputs are not shortened.

## Render and qualify

Build the router from the upstream release, verify its source identity and publish it in your registry. Retain upstream notices and record the resulting platform/image digest:

```bash
git clone --branch v0.3.0 --depth 1 https://github.com/NVIDIA-NeMo/Switchyard.git local/switchyard
# Confirm HEAD is 336196f6fbfc97ddc71c1700f6092e564e9f23c2 before building.
git -C local/switchyard rev-parse HEAD
docker build -t YOUR_REGISTRY/switchyard:0.3.0 local/switchyard
docker push YOUR_REGISTRY/switchyard:0.3.0
```

Render using the exact served model IDs and qualified shared context window. Replace the endpoint examples with your existing local model Services:

```bash
python3 deployment/routing/render.py \
  --efficient-endpoint http://nim-efficient:8000/v1 \
  --capable-endpoint http://nim-capable:8000/v1 \
  --efficient-model YOUR_EFFICIENT_SERVED_MODEL \
  --capable-model YOUR_CAPABLE_SERVED_MODEL \
  --router-endpoint "http://rag-switchyard.${NAMESPACE}.svc.cluster.local:4000/v1" \
  --context-window YOUR_QUALIFIED_SHARED_CONTEXT_WINDOW \
  --output local/routing

docker run --rm --runtime runc --network none \
  -v "$PWD/local/routing:/config:ro" YOUR_SWITCHYARD_IMAGE_AT_SHA256 \
  --config /config/routes.toml --dry-run

python3 deployment/routing/qualify_fixture.py \
  --image YOUR_SWITCHYARD_IMAGE_AT_SHA256 \
  --output local/routing-fixture-results.json
```

The fixture temporarily creates a loopback-only synthetic model server and CPU router on dynamically selected ports, then removes both. It verifies phase affinity, coding error recovery, job isolation and fixed aliases. It is a router-behavior test, not evidence of real model quality or speed.

Before activation, verify both real model endpoints and structured-output compatibility, streaming, context limits, held-out research grounding and executable coding acceptance. Compare fixed-capable, fixed-efficient and routed cases with the same inputs, warmed/cold-cache conditions stated, classifier cost included, and failures counted. A broken efficient endpoint or unqualified model pair keeps research routing disabled. No additional model is automatically deployed.

## Run:ai Helm deployment

Create a versioned ConfigMap, retaining it for any research jobs pinned to its policy:

```bash
export ROUTING_CONFIG=rag-routing-POLICY_HASH_PREFIX
kubectl -n "$NAMESPACE" create configmap "$ROUTING_CONFIG" \
  --from-file=routes.toml=local/routing/routes.toml \
  --from-file=research-client.json=local/routing/research-client.json \
  --dry-run=client -o yaml > local/routing-configmap.yaml
kubectl apply -f local/routing-configmap.yaml
```

Add these values to your private installation YAML after qualification:

```yaml
routing:
  enabled: true
  researchEnabled: true
  image: YOUR_SWITCHYARD_IMAGE_AT_SHA256
  configMap: rag-routing-POLICY_HASH_PREFIX
  nodeSelector: {}
```

Use the normal Helm AI Application upgrade flow in the Run:ai runbook. The ConfigMap's client endpoint must match the release name/namespace (`rag-switchyard` in the example). Only the ordinary research worker receives routing configuration. The router gets no datastore credentials, source storage, model weights, GPU resources or service-account token. It is cluster-internal; do not expose its model/statistics endpoints publicly. Its `/v1/models` readiness probe establishes configuration/service availability, not upstream inference readiness. Inspect the rendered manifest in the registered project before upgrading.

For native workspace lifecycle instead of Helm, `deployment/routing/native.py` prepares a CPU-only workspace request, Service, versioned ConfigMap and proposed research environment/storage additions from an existing reviewed native request. It never submits resources. Its output explicitly requires native admission policy to disable automatic service-account token mounting; verify the admitted pod, get/stop/run controls and inference before activation. Do not create duplicate Helm/native workers.

## Rollback

Keep old versioned configuration and image digests. Stop affected research admission/worker execution before switching policies. Restore the previous image and the exact policy/configuration for in-flight routed jobs, then resume them. A changed/missing policy fails explicitly rather than silently reclassifying a checkpointed phase. Disabling routing is appropriate for new jobs; already routed jobs require their pinned policy or explicit cancellation/new-job submission. The independent coding controller can select `coding-capable` for new jobs. Preserve completed reports, checkpoints, source data, model services and caches.

References: [Switchyard release](https://github.com/NVIDIA-NeMo/Switchyard/releases/tag/v0.3.0), [task routing](https://nvidia-nemo.github.io/Switchyard/routing_algorithms/llm_classifier_routing/), [Composite](https://nvidia-nemo.github.io/Switchyard/routing_algorithms/composite_routing/).
