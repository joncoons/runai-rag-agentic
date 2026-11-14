# Agentic RAG on NVIDIA Run:ai

Collection-based ingestion and search with NVIDIA Nemotron models, NeMo Retriever extraction, Elasticsearch and optional GPU-accelerated Milvus/CAGRA.

- Upload files, use administrator-approved directories or crawl scoped websites. Configure source schedules and track changes with content hashes.
- Ingest webpage text independently of linked binary downloads, image extraction and optional asynchronous captioning. Keep source/asset lineage and complete legal subtext.
- Use PostgreSQL transactions, an outbox, Redis Streams, bounded retries and independent writers to track publication. Elasticsearch receives every collection; selected collections also publish to Milvus. Copy existing collections to Milvus from collection management.
- Search across selected collections with a global top 10, rerank to 5 and stream the answer with source links. Select deep research explicitly. With no collection selected, use model-only answers.
- Manage sources, schedules, image-origin breakdowns and actionable errors. A dedicated local admin login controls approved directory aliases.

## Explore the interface

The [visual GUI walkthrough and field reference](docs/gui/README.md) follows a fictional Atlas Reference collection through uploads, website crawling, connected locations, schedules, search and admin controls. Screenshots are illustrative, with no production data or benchmark results.

[![Fictional scoped search with collection and index controls](docs/gui/images/13-search-results.png)](docs/gui/README.md)

## Deploy

Deploy with the [application/model Helm chart](charts/agentic-rag), optional [datastore chart](charts/agentic-rag-infrastructure), and [YAML values examples](deployment/helm). Follow the [Run:ai runbook](RUNAI_RUNBOOK.md) for project, storage, model and secret setup. The repository contains application/runtime source, synthetic tests, portable deployment templates and [flow diagrams](diagrams/README.md). Deployment values, credentials, downloaded content and model weights are supplied separately.

The qualified extraction library is `nemo-retriever==26.8.2`, with three hash-verified compatibility patches. NIM model templates use `:latest` and `Always`; record the resolved image/model revision before admitting new data. Updating a model does not make old vectors compatible automatically.

```bash
docker build -t YOUR_REGISTRY/agentic-rag:release .
docker run --rm --runtime runc --network none --entrypoint python \
  YOUR_REGISTRY/agentic-rag:release tools/test.py
```

The NVIDIA base image requires the appropriate registry access and NVIDIA/model terms. The build downloads dependencies and matching Chrome/ChromeDriver binaries; it does not require an existing application image from another cluster. Deployment targets Linux x86-64 and Python 3.12. Uploaded documents and webpages remain the source of record.

Elasticsearch and Milvus do not participate in a shared database transaction. PostgreSQL tracks each required projection and controls logical publication, retry and recovery. Readers see the committed collection view after the required receipts have been verified.

Document-level graph/abstract experimentation is outside this release. Temporary document analysis is available through the separately configured private workflow described below. External enterprise IAM integration is installation-specific; the included local admin is for local phase-1 operation.

Third-party notices are in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). No license file is supplied for original application code.

The left navigation separates **Search**, **Deep research** and **Document analysis**. Research is an explicit local collection job with persisted progress and a downloadable report. Document analysis provides an owned, expiring workspace, optional async PDF captions and an authorized Add to collection action. See the [GUI walkthrough](docs/gui/README.md#search-and-research-modes) and [private analysis setup](RUNAI_RUNBOOK.md#search-research-and-private-document-analysis). Private processing requires separately provisioned identities, storage and queues; it is disabled in Helm values until configured.

Deep research can optionally include Tavily web evidence through a default-off per-job control. See [web-search setup](RUNAI_RUNBOOK.md#optional-tavily-web-research). No provider credential is exposed to the GUI or private document analysis.

Optional [workload-specific routing](docs/WORKLOAD_ROUTING.md) provides task-based research phases and a Composite route for disposable coding harnesses through one CPU-only Switchyard service. Routing is disabled until the local model pair is qualified; ordinary search keeps its fixed streaming model.

[Three-model Search and remote-inference controls](docs/SEARCH_ROUTING.md) · [Search routing diagram](diagrams/search-model-routing.svg)

An optional [Lightning model and shared-GPU capacity example](docs/MODEL_CAPACITY.md) provides Run:ai fractions for embedding, Parse, Omni and Lightning while retaining a dedicated Super model. Checkpoint bytes and runtime caches are separate; model deployment and query-routing activation are independent.
