# Run:ai deployment runbook

## What is supplied

| Artifact | Purpose |
|---|---|
| `charts/agentic-rag` | Application workers, API/MCP Services, optional NIM models, configuration, storage and migrations |
| `charts/agentic-rag-infrastructure` | Optional dedicated PostgreSQL, Redis, Elasticsearch, Milvus GPU, etcd and object storage |
| `deployment/helm/values-external.yaml` | Complete application values for installation-specific external datastores/model endpoints |
| `deployment/helm/values-local.yaml` | Overlay for the optional local datastores and in-project model Services |
| `deployment/helm/values-models.yaml` | Explicit model enablement, fractions, checkpoint/cache claims and profiles |

Use Helm 3.14 or newer. Run:ai supports Helm charts as **AI Applications** in a registered project namespace; use the GUI's chart upload/custom chart URL, or Helm directly into that namespace. Application/model Deployments and datastore StatefulSets carry Run:ai scheduler/project labels and normal Helm release labels. These are Helm-managed workloads, distinct from the native workspace API examples retained under `deployment/runai/`. Do not deploy both variants into the same application/datastore state.

References: [Run:ai AI Applications](https://docs.nvidia.com/run-ai/self-hosted/v3.0/documentation/ai-applications/introduction-to-ai-applications), [uploading charts and YAML overrides](https://docs.nvidia.com/run-ai/self-hosted/v3.0/documentation/ai-applications/submit-ai-applications), [Run:ai fractional GPU YAML](https://run-ai-docs.nvidia.com/self-hosted/platform-management/runai-scheduler/resource-optimization/fractions). Check support and AI Application discovery in your installed Run:ai release. The supplied templates do not spoof native control-plane ownership labels.

## 1. Project, registry and persistent storage

Create or select a project in Run:ai and use its registered namespace. Set project quotas and confirm GPU registration, NVIDIA runtime and shared storage work. Set `runai.projectName` to the project queue name, not the namespace by assumption. Optionally provide its `projectId`. Do not install NVIDIA device-plugin time-slicing, Dynamo or KEDA for this deployment. Run:ai GPU fractions and its time-slicing configuration provide GPU sharing. Set `runai.gpuRuntimeClassName` to the cluster's NVIDIA runtime class, or `""` when the default runtime already supplies GPU integration.

```bash
git clone https://github.com/joncoons/runai-rag-agentic.git
cd runai-rag-agentic
export NAMESPACE=YOUR_REGISTERED_PROJECT_NAMESPACE
mkdir -p local
chmod 700 local
```

Authenticate privately to NGC and your image registry. Build from the public NRL image, then push and record the immutable application digest:

```bash
docker build -t YOUR_REGISTRY/agentic-rag:release .
docker push YOUR_REGISTRY/agentic-rag:release
```

Set `application.image` to `YOUR_REGISTRY/agentic-rag@sha256:ACTUAL_DIGEST`. The build uses the qualified Retriever26.8.2 library, verified patches and matched browser/driver downloads. Air-gapped installations must mirror these dependencies. Supply `imagePullSecrets` through chart values or project policy.

The charts create **PVCs only**, never hostPath PVs. Use a StorageClass that can satisfy the requested access modes. Application artifact/configuration storage must be shared across the selected nodes. Existing claims are the default; set each `storage.*.create: true` only for new dedicated storage. Created application/datastore PVCs have Helm's `keep` policy and survive uninstall. The charts do not delete or rebind existing data.

| Application value | Content / access |
|---|---|
| `storage.artifacts.claim` | Retained bytes, stages, captions and payloads; writable shared storage |
| `storage.source.claim` | Administrator-approved source files; mounted read-only |
| `storage.checkpoint.claim` | Embedding tokenizer/checkpoint input files; mounted read-only at `/model/embed` |
| `storage.checkpoint.subPath` | Optional checkpoint subdirectory within a shared engine-parent claim, e.g. `embed` |
| `storage.configuration.claim` | Tokenizer contracts and retained historical legal tokenizers; writable by preparation Job, read-only by workers |

Model checkpoint claims mounted at `/model` must contain the role directory (`embed/`, `rerank/`, `parse/`, etc.). The application checkpoint mount instead points directly at the three input files; use `storage.checkpoint.subPath` when sharing an engine-parent claim. Profile NIMs use the cache directory expected by that image.

## 2. Choose datastore setup

### Existing enterprise services

Use managed/existing PostgreSQL, persistent Redis, Elasticsearch and GPU-capable Milvus, including Milvus's etcd/object storage. Set `secrets.runtime` to an existing namespace-local Secret containing the keys in `config/runtime.env.example`; this chart does not embed passwords in values or Helm release history.

```bash
cp config/runtime.env.example local/runtime.env
chmod 600 local/runtime.env
# Fill endpoint/user/password values privately.
kubectl -n "$NAMESPACE" create secret generic rag-runtime --from-env-file=local/runtime.env
kubectl -n "$NAMESPACE" create secret generic rag-es-ca --from-file=ca.crt=local/elasticsearch-ca.crt
```

For HTTPS Elasticsearch, set `secrets.elasticsearchCA` to the CA Secret. For an internal HTTP endpoint, set it to `""`; the chart removes the CA mount/environment variable. Use separate scoped reader/writer credentials in enterprise deployments. Set `MILVUS_TOKEN` to the required value, or retain the key with an empty value if Milvus authentication is disabled.

### Optional local datastore chart

This is a dedicated local, single-instance stack rather than an enterprise HA/TLS configuration. It uses password-protected PostgreSQL/Redis/Elasticsearch and private namespace-only datastore ingress. Elasticsearch HTTP and Milvus/etcd/object-store transport are internal cleartext. Use external TLS-managed infrastructure for enterprise exposure; do not expose these Services publicly. NetworkPolicy enforcement requires a compatible CNI.

The helper creates random local credentials as two Kubernetes Secrets without printing passwords or placing them in Helm values. It refuses to overwrite existing credentials. The local profile uses Elasticsearch's `elastic` account for both projections; use scoped credentials for enterprise operation.

```bash
python3 tools/local_secrets.py --namespace "$NAMESPACE" --data-release rag-data
cp deployment/helm/values-infrastructure.yaml local/infrastructure.yaml
# Set runai.projectName, storageClass, resource/placement requirements and optional imagePullSecrets.
helm upgrade --install rag-data charts/agentic-rag-infrastructure \
  --namespace "$NAMESPACE" -f local/infrastructure.yaml \
  --wait --timeout 20m
```

Milvus defaults to one full GPU using the Run:ai scheduler. Its etcd/object-store components request no GPUs. A fraction can be configured explicitly if measurements support it. Qualified images are pinned for datastore compatibility; Milvus uses `milvusdb/milvus:v3.0.5-gpu`, never the CPU `latest` image. Both VDBs use 2048-dimensional float32 unit vectors/IP; Milvus must support `GPU_CAGRA`.

## 3. Admin identity and model assets

```bash
python3 tools/admin_config.py --username YOUR_ADMIN_NAME --output local/admin.json
kubectl -n "$NAMESPACE" create secret generic rag-local-admin --from-file=config.json=local/admin.json
```

The password is prompted and stored only as a salted scrypt verifier. No default password is shipped. The Admin view can register aliases only inside operator-approved read-only mounts. Set `configuration.allowedSourceRoots` to admit mounted roots, for example:

```yaml
configuration:
  allowedSourceRoots:
    approved: /source-roots/approved
```

Download the selected model checkpoints/profiles using the NIM image's documented mechanism. These assets and tokenizer files are not included in the chart. Checkpoint engines may be shared across compatible GPUs; compiled caches must be separated by:

```text
GPU_SKU/SM/DRIVER/MODEL/IMAGE_DIGEST/CHECKPOINT_REVISION/PRECISION/EXECUTION_HASH
```

Create and populate each model's checkpoint/cache PVC and cache subdirectory before enabling it. Model `nodeSelector`, profile and GEMM backend must match the actual hardware. The qualified NVFP4 embedding checkpoint requires compatible Blackwell hardware/backend; it must not be assumed to work on Ada simply because both are RTX GPUs. Super VL uses TP1 on one full compatible GPU. Select a profile that actually supports that execution configuration.

Use `deployment/helm/values-models.yaml` as an explicit overlay. It covers embed, rerank, parse, page elements, table structure, OCR, optional legacy graphic elements, Nano Omni captioning and Super VL. Page elements and table structure use **separate replicas of the same Nemotron Object Detection image** with different engines. Current native chart/infographic extraction uses page detection/OCR and optional asynchronous captioning; the graphic-elements route is retained for qualified legacy use.

Every NIM image remains `:latest` with `imagePullPolicy: Always`. Mirror-image overrides must retain that tag. Record actual resolved image/checkpoint digests for provenance, requalify model changes and never silently mix embedding revisions in a collection. Choose fractions from measured peak memory/concurrency. Fractional pods use Run:ai's annotations; full-GPU pods request an integer GPU. No separate device-plugin sharing configuration is installed.

If a NIM needs an API key to resolve/download its selected model, provide a Secret reference through `extraEnv`; registry image-pull credentials alone do not supply the model API key:

```yaml
imagePullSecrets:
  - name: ngc-registry
models:
  caption:
    extraEnv:
      - name: NGC_API_KEY
        valueFrom:
          secretKeyRef:
            name: nim-api-key
            key: NGC_API_KEY
```

When deploying models through this chart, point the endpoint bindings at their Services:

```text
http://RELEASE-model-embed:8000/v1/embeddings
http://RELEASE-model-rerank:8000/v1/ranking
http://RELEASE-model-caption:8000/v1/chat/completions
http://RELEASE-model-llm:8000
```

`values-local.yaml` supplies extraction/retriever/caption endpoints for release name `rag`; set `bindings.LLM_ENDPOINT` separately. If using another release name, update every endpoint accordingly. Existing internal model endpoints or an operator-managed authentication proxy may be used instead of chart-managed models. The application does not inject model API authentication headers.

## 4. YAML application configuration and installation

```bash
cp deployment/helm/values-external.yaml local/application.yaml
# Fill runai.projectName, application.image, Secret/PVC references and bindings.
# Optional: copy/edit the local and model overlays instead of modifying the originals.
cp deployment/helm/values-local.yaml local/local.yaml
cp deployment/helm/values-models.yaml local/models.yaml
```

The full example lists required input values. Replace every `REPLACE_*` value that is actually used; Helm rejects unresolved placeholders. Supply real checkpoint/content manifests and platform image digests. The chart derives duplicate embedding/index identity aliases and their canonical config hashes from the selected checkpoint. Truncation remains disabled and embedding dimensions remain 2048. The platform image digest fields use `sha256:...`; checkpoint manifest fields use the bare 64-character hex digest.

Configuration is rendered into a small ConfigMap for bindings, model contracts and allowed source roots. Large tokenizer files and historical input contracts stay on `storage.configuration.claim`, not in a ConfigMap. Choose one preparation method:

- Set `configuration.prepareTokenizers: true` to run the preparation Job using the checkpoint's three input files. Verify `configuration.maxRequestCharacters` against the serving model; its initial value reflects the qualified model limit, not an application payload ceiling.
- Or prepare a configuration directory with `tools/tokenizer_contract.py` and populate the configuration PVC privately before startup. Preserve the complete historical contract directory on later upgrades.

Migrations run as an ordinary bounded Kubernetes Job for each Helm revision, not a pre-install hook that would need uncreated PVCs. Consumers/API wait for the exact image's migration ledger and valid tokenizer contract in init containers, avoiding a Helm `--wait`/post-install-hook deadlock.

```bash
helm lint charts/agentic-rag -f local/application.yaml
helm template rag charts/agentic-rag --namespace "$NAMESPACE" \
  -f local/application.yaml > local/rendered-application.yaml
helm upgrade --install rag charts/agentic-rag \
  --namespace "$NAMESPACE" -f local/application.yaml \
  --wait --wait-for-jobs --timeout 30m
```

For the local stack and chart-managed models, add `-f local/local.yaml -f local/models.yaml` **to lint, template and install alike**, after `-f local/application.yaml`. Values files later in the list override earlier ones. Fill the overlay image/project placeholders too. NIM cold starts may need a longer timeout. Preflight all checkpoint claims and GPU capacity before enabling every model.

The chart starts 15 CPU application roles: API, query MCP, relay, admission, writer, file foreground, caption inference, refresh, caption admission, page extraction, image extraction, binary extraction, page admission, asset admission and asset download. These queues remain separate; linked assets/captions do not serialize webpage processing. Caption work is admitted only when enabled by the user, with bounded durable retries/backoff.

## 5. Run:ai GUI and chart packages

Package the chart sources locally:

```bash
mkdir -p local/packages
helm package charts/agentic-rag --destination local/packages
helm package charts/agentic-rag-infrastructure --destination local/packages
```

Published versioned packages:

- [Application/models 0.1.0](https://github.com/joncoons/runai-rag-agentic/releases/download/v0.1.0/agentic-rag-0.1.0.tgz)
- [Optional datastores 0.1.0](https://github.com/joncoons/runai-rag-agentic/releases/download/v0.1.0/agentic-rag-infrastructure-0.1.0.tgz)
- [SHA256 checksums](https://github.com/joncoons/runai-rag-agentic/releases/download/v0.1.0/SHA256SUMS)

In **Workload manager → AI applications → New AI application**, select the project/cluster and upload the versioned `.tgz`, or supply a direct package URL from the repository's [releases](https://github.com/joncoons/runai-rag-agentic/releases). Paste your private YAML overrides into the application override form. Deploy the datastore chart first if needed, then the application chart. Existing Secret references keep passwords out of the values form.

Check that the Helm release appears as an AI Application, its components appear in Workloads, and Run:ai reports their actual queue/GPU allocations. Manage the Helm release consistently through that application workflow or Helm; avoid changes to generated child pods. Discovery/UI lifecycle behavior must be validated against the installed Run:ai version; static chart validation does not establish live GUI behavior.

```bash
kubectl -n "$NAMESPACE" get pods,jobs,pvc,services
kubectl -n "$NAMESPACE" logs job/rag-migration-1
kubectl -n "$NAMESPACE" get endpointslices
kubectl -n "$NAMESPACE" port-forward service/rag-api 8080:8080
```

Open `http://localhost:8080`. Supply your own ingress/TLS and disable response buffering for streamed search. Services default to ClusterIP. No database/model administration endpoint is exposed to end users.

## 6. Acceptance

1. Confirm successful migration/tokenizer Jobs and ready API/MCP/model endpoints.
2. Create a collection and upload a generated document; confirm metadata, completion and source links.
3. Crawl a scoped synthetic site with linked PDF/image assets and captions enabled; verify page text publishes independently of the asset queues.
4. Select “also write to Milvus”; confirm PostgreSQL records verified receipts for both projections. Duplicate an existing Elasticsearch collection to Milvus and search each backend.
5. Change/remove an asset and recheck the source; verify provenance, current-parent fences, upserts and deletions. Edit its schedule in source management.
6. Verify global top 10/rerank 5, streamed answers and linked sources. With no collections selected, verify model-only answers. Deep research is user-selected.
7. Sign in as local admin, register an alias inside the approved mount and verify unapproved paths are rejected.

Elasticsearch and Milvus do not participate in one distributed ACID transaction. PostgreSQL controls logical publication after the required per-store receipts, and retries incomplete operations. Preserve the coordinator database and retained artifacts together.

## 7. Recovery and rollback

- **Init container waiting:** inspect migration Job logs and tokenizer preparation/PVC contents. Migrations record immutable SQL file digests and reject changed applied migrations. Missing input files cannot be repaired by restarting every worker.
- **MCP unavailable:** check query-MCP pods, Service/EndpointSlices and URL; scoped search reports failure instead of silently changing retrieval scope.
- **Caption pending after inference:** inspect parent publication, caption admission and writer receipts before replaying model work. Deleted/undersized assets are not actionable failures.
- **Partial VDB writes:** retry the coordinator operation; do not manually edit one VDB to simulate committed state.
- **GPU pending/OOM:** verify queue quota, node/profile/SM compatibility, cache ownership and measured memory. Do not install a competing GPU sharing plugin.

Back up PostgreSQL, retained artifact/configuration PVCs, model/checkpoint identities and required ES/Milvus/object-store/Redis persistence as one coordinated recovery set. Pause producers and record consumer state first. Restore into an isolated project and validate lineage/receipts before resuming.

Use `helm history rag -n "$NAMESPACE"` and a previously qualified chart/image/values set for rollback. Do not assume Helm rolls back database schemas, external VDB writes or model caches. Pause ingestion for incompatible model/schema changes; retain historical tokenizer contracts. Uninstall retains the dedicated PVCs; deleting them requires an explicit operator data-retention decision. Never remove shared caches or historical namespaces as incidental cleanup.

The older native-workspace API helper remains an alternative for operators choosing that lifecycle. It is not required for Helm AI Applications and must not create duplicate consumers against the same queues.


### Image content reuse upgrades

Run the additive application migration job before starting the updated caption worker; it creates the image-caption cache tables. Use the same application build for the API and all admission/writer roles. Update publication proof readers before resuming the new caption-admission build: existing caption admissions remain valid under their original identity policy. New admissions collapse identical image content within a parent document while retaining historical admissions and source/region links.

Caption generation pending and captions awaiting publication are separate GUI counters. Generated captions become searchable only after parent-source admission and selected-store publication receipts. Identical image bytes reuse successful caption/vector inference within a collection only when the effective model/processing contract matches; failed or incomplete inference is not cached. Similar-looking images with different bytes are distinct inputs.

Admission and writer memory must accommodate the retained publication artifacts. The supplied chart requests should be reviewed against the selected corpus and node capacity; out-of-memory restarts preserve the external durable queue but stop publication progress.

## Search, research and private document analysis

The left navigation separates **Search**, **Deep research** and **Document analysis**. Ordinary search keeps the global top-10 retrieval / top-5 reranking behavior. Deep research starts an owned durable local job; the user chooses collections, the search index and its search budget. It does not grant internet access unless the separately configured per-job web-search option is selected. A CPU research worker retains checkpoints and report deltas in PostgreSQL; Redis delivers notifications and database leases recover missed notifications or stopped processes.

Document analysis is opt-in infrastructure. It uses the existing qualified ingestion and NIM endpoints, with separate CPU extraction, caption, admission, writer, research, cleanup and scoped-MCP workers. Captions remain optional and asynchronous. No additional GPU allocation or device-plugin time-slicing configuration is introduced.

Before enabling it, the operator must provide:

1. A **separate PostgreSQL database and login role**, restricted to that database. The private role must not read permanent application tables. Exclude the entire private database from ordinary backups; do not include it through an unrestricted `pg_dumpall` backup.
2. An **independent Redis instance**, not another logical database on permanent Redis. The chart starts one using an existing configuration Secret. Configure `dir /analysis-private/redis`, `appendonly yes`, `appendfsync always` and authentication. Use the same password in the private Redis URL. NetworkPolicy limits inbound Redis traffic to the API/private workers in this release; network enforcement requires the cluster's policy controller.
3. A **dedicated artifact PVC** outside permanent source/artifact storage and its backup paths. Mount it at `/analysis-private`; runtime UID/GID 1000 must be able to create private `workspaces` and `redis` directories. Retention applies to this private storage, not to common model checkpoints or GPU caches.
4. Dedicated Elasticsearch identities scoped only to **`rag_analysis_*`** indexes. The reader needs `read` and `view_index_metadata`; the writer additionally needs `write`, `create_index`, `delete_index` and index-management privileges needed for projection refresh/settings. Do not grant private identities access to permanent `rag_agentic_*` indexes. Provide the same trusted ES endpoint/CA already configured for the application.
5. An existing runtime Secret with these exact keys:

   | Key | Value supplied by the operator |
   |---|---|
   | `PRIVATE_POSTGRES_DSN` | Connection URL for the separate private database/role. |
   | `PRIVATE_REDIS_URL` | Authenticated URL for this release's `RELEASE-analysis-redis.NAMESPACE.svc.cluster.local:6379/0`. |
   | `PRIVATE_ES_READER_USER`, `PRIVATE_ES_READER_PASSWORD` | Private-index read identity. |
   | `PRIVATE_ES_WRITER_USER`, `PRIVATE_ES_WRITER_PASSWORD` | Private-index writer/cleanup identity. |

   Also provide a Redis configuration Secret with a `redis.conf` key, and a scope-signing Secret with a random `key` of at least 32 bytes. Mounting or logging provider/administrator credentials in agent prompts is never required. Keep Secret values and any local credential files out of Git.

Complete `deployment/helm/values-private-analysis.yaml` with **Secret/PVC names**, then review an offline rendering:

```bash
helm template rag charts/agentic-rag --namespace REPLACE_NAMESPACE \
  -f PRIVATE_BASE_VALUES.yaml \
  -f deployment/helm/values-private-analysis.yaml > PRIVATE_RENDERED.yaml
```

Inspect the rendering before your normal approved Helm upgrade. The chart adds a private schema Job plus eight private CPU deployments (seven processing/MCP roles and Redis). Existing regular migrations install research/promotion tables. Workers wait for the private schema. The private backend never receives permanent database, permanent Redis, or permanent ES writer credentials. Model/tokenizer readiness and actual Run:ai lifecycle acceptance remain operator qualification gates; chart rendering alone does not establish a working deployment.

Private workspaces default to **24 hours**. Ownership is tied to the browser session; copying a workspace URL does not grant access. Expiry or **Delete now** immediately fences HTTP/MCP/downloads and late publication. The cleanup worker stops/reaps expired work, removes scoped indexes, files, queue entries and content-bearing database rows, and leaves a content-free lifecycle record. Do not back up private working state into permanent corpus backups.

**Add to collection** requires an existing explicit destination write grant in local admin configuration. Signing in alone does not supply that grant, and installation never adds `*` automatically. The action copies original bytes into ordinary durable intake. Its status becomes complete only after the external writer verifies every selected store, including Milvus when enrolled. A copy already accepted by the destination survives private expiry. Revocation fences new publications, including late captions, while preserving previously published source evidence.

Qualification before sharing should cover upload/answer/source download, independent captions, owner isolation, immediate deletion and eventual physical cleanup, allowed/denied promotion, both selected-store receipts, local research cancellation/resume, stream reconnection and report download. A generic Helm install on another cluster has not been runtime-qualified by the repository's local tests.

The new workflow chart source is version 0.2.0. Its private option has been rendered and contract-tested locally; a new packaged/tagged release has not been published. Use the chart source above when qualifying these workflows. Existing 0.1.0 release assets contain the earlier application layout.

### Private-workflow acceptance checks

Before enabling the private navigation in a new installation, verify owned document upload/question/download; a second session must receive 404. Stop the separate caption worker during an image-bearing PDF test: text and grounded answers must become ready independently, then captions publish after the worker resumes. Delete the workspace and verify access is fenced immediately and its files, rows and private indexes are subsequently purged. Private Elasticsearch roles remain restricted to `rag_analysis_*`; cleanup uses scoped index metadata and does not require cluster-monitor privileges.

Run a bounded collection research job, reconnect its event stream and download the completed report. Submit a synthetic ordinary upload to a destination enrolled in both Elasticsearch and Milvus: PostgreSQL must record both matching receipts before publication. Promotion requires an explicit destination write grant; do not grant wildcard access for acceptance. Confirm an active ordinary-file extraction consumer before enabling uploads/promotions; avoid adding a second consumer if the foreground deployment already serves that stream.

### Optional Tavily web research

Deep research stays collection-only by default. To offer the per-job **Include web search** choice, create a namespace-local Secret containing `TAVILY_API_KEY` and set these values. Enter the credential with your secret-management tooling; keep it out of values files and shell history.

```yaml
research:
  webSearch:
    enabled: true
    secretName: REPLACE_WEB_SEARCH_SECRET
    secretKey: TAVILY_API_KEY
```

Only the ordinary research worker receives the provider credential. The API receives an enablement flag; private-analysis workers receive neither. The installation needs HTTPS egress to `api.tavily.com`. Disabling this flag prevents new opted-in requests; stop active research before removing egress or credentials if those jobs must not continue. Private document analysis never enables web search.

The unchecked option resets after each successful job submission. It supports web-only jobs with no collection selected, or selected collections plus web. Only the question and question-only planned search terms go to Tavily; retrieved collection passages and private files are not sent. The existing search budget bounds planned queries; each query can search the selected collections and Tavily (five web results per query). Explicit basic search depth and no provider-generated answer keep final synthesis in the local model.

PostgreSQL checkpoints retain the opt-in, provider steps and excerpt evidence with URL/title, content/response hashes, the original successful provider-response bytes (checkpoint-only, not client/model input), provider request identity and capture time. Resuming skips committed provider steps. A crash after an external response but before checkpoint commit may repeat that provider request; provider billing is not exactly-once. Existing finite job retries apply. Provider failures remain visible, without silently substituting local/model-only research. Reports mark external excerpts and retain source links; these are not complete webpage captures and are never automatically written into collections. Following links to analyze full PDFs in job-owned ephemeral collections is a separately planned tool extension.
