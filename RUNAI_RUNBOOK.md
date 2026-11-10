# Run:ai runbook

## 1. Prerequisites and project

Use a working Kubernetes cluster with NVIDIA Run:ai, GPU drivers/runtime, shared persistent storage and an image registry accessible to the cluster. Create a project in the Run:ai GUI and use its registered namespace, project ID and cluster ID. Set project quotas for the intended workloads. Namespace creation alone does not register a Run:ai project.

Install the Run:ai cluster integration appropriate to your environment. Use Run:ai GPU fractions and its own time-slicing capabilities; this release does not install NVIDIA device-plugin time-slicing, Dynamo or KEDA. Fractions reserve GPU memory; compute time-slicing is a separate Run:ai setting. Measure peak memory and concurrent requests before choosing fractions.

Prepare PostgreSQL, persistent Redis Streams, Elasticsearch with TLS and a GPU-capable Milvus server with its etcd/object-storage dependencies. Endpoints and credentials are installation-specific. The application uses 2048-dimensional float32 unit vectors with inner-product similarity in both stores. Milvus must support `GPU_CAGRA`; CPU-only Milvus is insufficient. `milvusdb/milvus:v3.0.5-gpu` is the qualified server image; do not replace it with the CPU `latest` tag. Existing external services may be used. Provisioning these infrastructure services is an operator prerequisite, not performed by the application.

Reference: [Run:ai workspace setup](https://run-ai-docs.nvidia.com/self-hosted/workloads-in-nvidia-run-ai/using-workspaces/running-workspace), [GPU fractions](https://run-ai-docs.nvidia.com/self-hosted/platform-management/runai-scheduler/resource-optimization/fractions), [native workspace API](https://docs.nvidia.com/run-ai/self-hosted/reference/rest-api/endpoints/workloads/workspaces/create-workspace-1). Match the API schema to your installed Run:ai release.

## 2. Build and publish the application image

Authenticate to NGC and your registry using credential helpers or password-stdin. Keep credentials out of shell history and Git. Use a private Docker configuration if needed.

```bash
git clone https://github.com/joncoons/runai-rag-agentic.git
cd runai-rag-agentic
docker build -t YOUR_REGISTRY/agentic-rag:release .
docker push YOUR_REGISTRY/agentic-rag:release
```

Record the pushed immutable digest as `APPLICATION_IMAGE` in your private values file. The public build starts with `nvcr.io/nvidia/nemo-microservices/nrl-service:26.8.2`, installs hash-locked dependencies and verifies the Retriever wheel and patches. It downloads a matched Chrome/ChromeDriver pair for the original crawler's Selenium fallback. For an air-gapped installation, mirror these public dependencies before building.

```bash
docker run --rm --runtime runc --network none --entrypoint python \
  YOUR_REGISTRY/agentic-rag:release tools/test.py
```

Integration tests require disposable PostgreSQL and model/tokenizer inputs; they must not target your production database. See test-suite environment variables before enabling them.

## 3. Storage and secrets

Provision dedicated existing PVCs and populate them before startup. All must be accessible from the selected worker nodes; application processes use UID/GID 1000.

| Value | Purpose | Application access |
|---|---|---|
| `ARTIFACT_PVC` | Retained source bytes, stages, caption assets and payloads | Read/write across workers |
| `SOURCE_PVC` | Administrator-approved source root | Read-only at `/source-roots/approved` |
| `CHECKPOINT_PVC` | Embedding tokenizer/checkpoint input files | Read-only at `/model/embed` |
| `CONFIG_PVC` | Rendered config, tokenizer input contracts and legal tokenizers | Read-only at `/app/config` |
| `MODEL_CHECKPOINT_PVC` | Model-specific universal checkpoints | As required by the NIM family |
| `MODEL_CACHE_PVC` | GPU/runtime-specific writable caches | Read/write |

A model's checkpoints can be shared across compatible GPUs. Use a different runtime-cache subdirectory for each combination of model revision, GPU SKU/SM, driver, image platform digest, precision and execution configuration. For example:

```text
GPU_SKU/SM/DRIVER/MODEL/IMAGE_DIGEST/CHECKPOINT_REVISION/PRECISION/EXECUTION_HASH
```

`GPU_CACHE_SUBPATH` is private deployment configuration. Create its directory and permissions in advance. Do not reuse compiled caches across incompatible GPUs. Profile-based NIMs may need a writable NIM cache in addition to the separately mounted compilation cache; follow the selected image's model-download contract.

```bash
mkdir -p local rendered
chmod 700 local
cp config/runtime.env.example local/runtime.env
chmod 600 local/runtime.env
# Edit local/runtime.env privately. Set NAMESPACE to the Run:ai project namespace.
kubectl -n "$NAMESPACE" create secret generic rag-runtime --from-env-file=local/runtime.env
kubectl -n "$NAMESPACE" create secret generic rag-es-ca --from-file=ca.crt=local/elasticsearch-ca.crt
python3 tools/admin_config.py --username YOUR_ADMIN_NAME --output local/admin.json
kubectl -n "$NAMESPACE" create secret generic rag-local-admin --from-file=config.json=local/admin.json
```

Configure registry image-pull credentials through Run:ai/project policy. The local admin password is prompted and stored only as a salted scrypt verifier. No default password is provided. Use separate Elasticsearch reader/writer users. The writer needs create/index/read/refresh permissions for application-owned physical generations; the reader needs read/search only. Milvus credentials need the equivalent scoped rights. Supply Redis authentication in `REDIS_URL` when required.

## 4. Model endpoints and placement

`deployment/runai/models/*.json.template` contains native workspace request examples for embedding, reranking, parse, page elements, table structure, OCR, graphic elements and captioning. Every NIM image uses `:latest` with `imagePullPolicy: Always`. Page elements and table structure use **the same Nemotron Object Detection image**, with different checkpoint/model identities. Preserve separate replicas and endpoint bindings. Graphic elements remains available for the qualified legacy path; current native chart/infographic extraction uses page detection and OCR, with optional asynchronous captioning.

Render each model template with private model-specific PVCs/profile/cache values using `tools/render.py`. Review CPU/memory, Run:ai GPU fraction, node-pool placement and supported precision before submission. The example fraction is a starting configuration, not a memory guarantee. Select a compatible profile/GEMM backend for the actual GPU architecture; do not infer SM compatibility from similar product names.

Checkpoint mode mounts `/model` with the engine at `/model/MODEL_ROLE`. Profile mode uses the NIM cache path expected by that image. Download checkpoints/profiles using the selected NIM image's documented download mechanism, then confirm model readiness and the served model name. Keep deployment-specific image/model digests and configuration identities in the private values file for provenance. Resolve `latest` at qualification; updates require new validation and must not silently reuse incompatible vectors.

Provide the LLM separately through `LLM_ENDPOINT`, using an OpenAI-compatible streaming chat endpoint. The intended model is Nemotron 3.5 Super VL NVFP4 TP1 on a compatible GPU. `CAPTION_URL` is a separate Nano Omni endpoint. `CAPTION_EXTRA_BODY` disables reasoning-only output for the qualified caption path. An application workload's CPU request does not reserve GPU memory for its remote NIMs.

Create Kubernetes Services pointing to native model workspace pods, or use internal endpoints reachable by these workers, or an operator-managed authentication proxy. For a native workspace named `rag-model-embed`, a Service selector `workloadName: rag-model-embed` exposes port 8000. Validate EndpointSlices before using its URL. Set both the binding-file endpoint and corresponding worker URL values consistently.

## 5. Render configuration and initialize schema

```bash
cp config/values.example.json local/values.json
# Fill every REPLACE_* value with this installation's real endpoints, identities and resources.
python3 tools/prepare_deployment.py --values local/values.json --output rendered
python3 tools/tokenizer_contract.py \
  --checkpoint /PATH/TO/EMBEDDING_CHECKPOINT \
  --checkpoint-sha256 ACTUAL_64_CHARACTER_CHECKPOINT_MANIFEST_SHA256 \
  --max-request-characters VERIFIED_SERVING_MODEL_CHARACTER_LIMIT \
  --output rendered/config
```

The tokenizer tool copies only the three input-contract files into the private configuration directory and hashes them. It disables no server limits; use the actual serving model's limit. Embedding and reranking requests use `truncate: NONE`. A section that fits the model context remains intact; overlength handling must preserve full source text and explicit provenance. Model/checkpoint/config digests must describe the assets actually served, rather than copied values from another installation.

Copy `rendered/config/` onto `CONFIG_PVC` using your storage operator workflow. The full directory hierarchy is required; large tokenizers are unsuitable for a single ConfigMap. Copy the embedding checkpoint's input files onto `CHECKPOINT_PVC` at its mount root. The source-root inventory starts empty. To enable directory registration, replace `location-roots.json` with `{"approved":"/source-roots/approved"}`; the authenticated admin can then register aliases inside this read-only mount. Never admit arbitrary host paths from a browser request.

Get a short-lived Run:ai token using your configured authentication provider and load it privately as `RUNAI_TOKEN`. Set `RUNAI_URL` to the HTTPS control-plane URL and optionally `RUNAI_CA_FILE` to its CA. The helper verifies TLS and never recovers cluster administrator credentials automatically.

```bash
python3 tools/runai_workspace.py create --request rendered/workspaces/migration.json
```

Wait for successful migration completion in Run:ai and inspect its logs. The migrator initializes durable, collection, caption and application schemas in order and records each file's digest. Re-running skips completed migrations; modifying an applied file is rejected. Back up existing data before applying schema changes.

## 6. Start the runtime

Create the query MCP and API workspaces, then the independent processing roles. Every request is a native Run:ai workspace so its lifecycle appears in the GUI. Keep the completed migration workspace stopped.

```bash
python3 tools/runai_workspace.py create --request rendered/workspaces/query-mcp.json
python3 tools/runai_workspace.py create --request rendered/workspaces/api.json
kubectl apply -f rendered/services.json
```

Create `relay`, `admission`, `writer`, `foreground`, `caption`, `refresh`, `caption-admission`, `page-extraction`, `image-extraction`, `binary-extraction`, `page-admission`, `asset-admission` and `asset-download` the same way. Start consumers before submitting a source. The foreground role handles ordinary file extraction; page, binary, image/download and caption roles remain separated so slow assets do not serialize webpage ingestion. Caption work is created only when enabled in source options, with persisted bounded retries and backoff.

The helper prints the returned workload ID. Keep it privately for subsequent lifecycle operations. API-version validation is required on your cluster; exported templates were structurally checked, not submitted to a second installation as part of publication.

```bash
kubectl -n "$NAMESPACE" get pods,pvc,services
kubectl -n "$NAMESPACE" get endpointslices
kubectl -n "$NAMESPACE" port-forward service/rag-agentic-api 8080:8080
```

Open `http://localhost:8080`. Verify `/api/status` and query-MCP readiness. For shared access, supply your ingress/TLS policy and disable proxy buffering for search streaming. Do not expose PostgreSQL, Redis or model administration endpoints through the user interface.

## 7. Acceptance and daily operation

1. Open or create a collection. Add a small synthetic file and confirm completion, source metadata and linked citations.
2. Add a scoped synthetic website with linked image/PDF assets, captions enabled. Confirm webpage text publishes independently, with later asset/caption events.
3. Enable “also write to Milvus”; verify both projections reach the committed state. Duplicate an existing Elasticsearch collection to Milvus and check search on both backends.
4. Update and remove a source asset, recheck the source, and confirm revision/upsert/delete tracking. No stale asset should remain current merely because its bytes are retained for provenance.
5. Edit a source's recheck schedule from collection management. Verify checksums prevent unchanged content from being re-extracted.
6. Search selected collections; verify global top 10/rerank 5, streamed text and source hyperlinks. With none selected, verify model-only operation. Deep research must be user-selected.
7. Sign in as local admin, register a mounted directory alias, then verify users can select the alias while unapproved paths remain rejected.

Collection publication is coordinated in PostgreSQL outside the vector databases. It records each required write/verification receipt and publishes the logical view after all required projections are ready. Recovery retries incomplete projections. This is not distributed ACID across Elasticsearch and Milvus, and operators must preserve the coordinator database along with artifacts.

## 8. Troubleshooting, backup and rollback

- **Scoped MCP unavailable:** check the query-MCP workspace, Service selector, EndpointSlices and `QUERY_MCP_URL`. The API intentionally reports scoped retrieval failure rather than silently switching to direct unscoped retrieval.
- **Caption pending after inference:** inspect caption admission, parent source publication and writer receipts. Generated captions may await publication; avoid rerunning successful inference. Deleted/undersized assets are not actionable caption failures.
- **Webpage waits for assets:** confirm dedicated page/image/binary/download/caption consumers are running and reading their distinct streams. Do not collapse them into a single serial worker.
- **Partial VDB writes:** inspect the PostgreSQL operation/projection state and retry the existing operation. Do not edit one VDB directly to make the GUI look committed.
- **Slow search:** measure embedding, retrieval, rerank and generation separately. Preserve one global top 10 across selected collections and rerank to 5. Source-catalog refresh runs outside the request path.
- **Model OOM or cache errors:** inspect actual Run:ai placement, concurrent memory usage, profile/SM compatibility and cache permissions. Choose fractions from measurements; do not add competing GPU scheduling plugins.

Back up PostgreSQL and retained artifact/configuration storage as a consistent recovery set. Also retain required Elasticsearch/Milvus snapshots, object storage, Redis persistence and model/checkpoint identities according to installation policy. Pause producers, let or pause consumers at a recorded checkpoint, then capture the coordinated state. Keep backup storage external to the application PVCs. Restore into an isolated namespace/project and validate lineage/projection receipts before resuming. Retained tokenizer contracts are required to replay historical legal grouping.

For application rollback, stop only this project's native workspaces, retain all existing non-ephemeral PVCs, and recreate requests using a previously qualified application image digest. Do not run destructive database downgrades automatically. Restore a consistent backup if a schema change is incompatible. Stop/run/delete through Run:ai or `tools/runai_workspace.py`; the helper checks the request's name and project before mutating a workload. Never delete shared caches or historical namespaces as part of an application rollback.
