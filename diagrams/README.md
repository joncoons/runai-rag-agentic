# Functional flows

These Mermaid diagrams describe the included runtime. Raw `.mmd` sources are linked below:

- [Search and streaming](simple-search-flow.mmd)
- [Independent webpage and asset processing](independent-web-ingestion.mmd)
- [Asset lifecycle](asset-lifecycle.mmd)
- [Collection publication](collection-publication.mmd)
- [Provenance](provenance-flow.mmd)
- [Source rechecks](source-rechecks.mmd)
- [Images, captions and approved locations](visual-caption-location-flow.mmd)

## Asset lifecycle

```mermaid
flowchart TD
    OBS[Complete page and asset observation]
    DIFF[Diff committed dependency manifest]
    PG[(PostgreSQL revisions / edges / operations / outbox)]
    PROC[Nv-ingest changed assets and rebuild parent evidence]
    STAGE[Stage page / owned documents / ES and optional Milvus]
    PUB[Atomic PostgreSQL publication: VDB handles and asset manifest]
    READ[Managed query and asset-serving gate]
    GC[Reference / retention / reader / job gates]
    PURGE[Fenced physical purge and durable receipt]
    OBS --> DIFF
    DIFF --> PG
    PG --> PROC
    PROC --> STAGE
    STAGE --> PUB
    PUB --> READ
    PUB --> GC
    GC --> PURGE
    PURGE --> PG
```

## Collection publication

```mermaid
flowchart TD
    API[Collection create / duplicate / upsert / delete]
    PG[(Existing PostgreSQL database<br/>collection_control schema)]
    OUT[Transactional domain outbox]
    WORK[Renewable lease and fenced attempt<br/>immutable content-addressed record versions]
    ES[Persistent Elasticsearch projection<br/>incremental verified versions]
    MV[Persistent Milvus native TEXT / vector / metadata<br/>incremental verified versions]
    CHECK[Validate new versions and per-store receipts]
    PUB[Atomic PostgreSQL authorized-version manifest]
    READ[Query / MCP filter published versions BEFORE top-k<br/>revalidate before evidence release]
    STAGE[Local EMBEDDING stage and attempt commit]
    ADMIT[Retained bytes and complete source-unit coverage<br/>upstream IDs and per-record stage proof]
    HTTP[HTTP first acquisition] --> JS{Sparse / empty main?}
    JS -->|yes| BROWSER[Original Selenium fallback and readiness wait]
    JS -->|no| RAW[Retained observation bytes and hashes]
    BROWSER --> RAW
    RAW --> UNITS[Versioned main-page and audience-disclosure units]
    UNITS --> STAGE
    STAGE --> ADMIT
    ADMIT -->|same PostgreSQL transaction| PG
    API --> PG
    PG --> OUT
    OUT --> WORK
    WORK --> ES
    WORK --> MV
    ES --> CHECK
    MV --> CHECK
    CHECK --> RECHECK[Revalidate source / job / stage / intake proof<br/>hold source share locks through publication]
    RECHECK --> PUB
    PUB --> PG
    READ --> PG
    READ --> ES
    READ --> MV
    REJECT[Permanent admission rejection<br/>cancel intent, preserve prior publication]
    DELETE[Logical deletion denial barrier<br/>fence work, retain versions for governed cleanup]
    WORK -->|admission fails| REJECT
    REJECT --> PG
    API -->|delete| DELETE
    DELETE --> PG
```

## Independent web ingestion

```mermaid
flowchart TD
  W[Original queued webcrawler — scoped unlimited depth] --> P[HTML page discovery]
  P --> T[PostgreSQL transaction: page receipt + text job + asset intents; no corpus publication validation]
  T --> Q[Redis: page extraction]
  Q --> E[Whole-page embedding]
  E --> A[Ready-page admission; bounded proof-preserving batches]
  A --> V[Incremental immutable record-version writer]
  V --> ES[Elasticsearch receipt]
  V --> MV[Optional Milvus strong-visibility receipt]
  ES --> PG[PostgreSQL authorized-version switch]
  MV --> PG
  T --> D[Redis: deferred download work]
  D --> L[Leased downloads — bounded retries]
  L --> B[Binary extraction]
  L --> I[NV-Ingest image extraction]
  B --> AA[Asset admission — exact current parent fence]
  I --> AA
  AA --> V
  I --> C{Caption enabled and native minimum met?}
  C --> CQ[Redis: independent caption inference]
  CQ --> CA[Caption admission — current parent + consent fence]
  CA --> V
  AA --> U[Same page record enrichment or independent binary records]
  CA --> U

  PG --> R[Search filters exact published versions before top-k]
```

## Provenance flow

```mermaid
flowchart TD
    source["Source acquisition<br/>Curated URL / document / asset<br/>Run ID, scope, observed time"]
    revision["Source revision<br/>Retained raw / normalized refs<br/>SHA-256 + previous revision<br/>HTTP / canonical URI metadata"]
    chunk["Extraction + chunks<br/>Page, bbox, span, time anchors<br/>Parser / chunker / model config<br/>Exact text hash + upstream IDs"]
    gate["Provenance admission<br/>Schema + referential integrity<br/>Hash verification + coverage<br/>Unverified data quarantined"]
    index["ES + optional CAGRA index<br/>Shared revision / chunk identities<br/>Per-store readiness + watermark"]
    mcp["MCP evidence tools<br/>Bounded text / page images<br/>Preserve revision + chunk IDs<br/>Scope enforced by server"]
    answer["Agent / answer record<br/>Cited evidence IDs + query<br/>Model / prompt / route identity<br/>Claims checked against sources"]
    delta["Recrawl / delta lifecycle<br/>New revisions; retain history<br/>Revalidate changed evidence<br/>Supersede / retract explicitly"]
    entailed["Source-grounded derivation<br/>Entailments + evidence spans<br/>Extractor / validator versions<br/>Preserve source-revision joins"]
    synthetic["Synthetic gap-fill lineage<br/>Gap + generator job + config<br/>Seed references; explicit origin<br/>Separate from source evidence"]
    dataset["Dataset version manifest<br/>Explicit grounded / mixed role<br/>Input IDs, split policy, hashes<br/>Immutable version identity"]
    register["Downstream reuse<br/>Training / evaluation registration<br/>Data Store ledger artifacts<br/>Entity / MLflow pointers"]
    source --> revision
    revision --> chunk
    chunk --> gate
    gate --> index
    index -->|evidence| mcp
    mcp --> answer
    source --> delta
    delta -->|new observation| revision
    gate -->|qualified evidence| entailed
    entailed --> dataset
    synthetic -.->|explicit blend only| dataset
    dataset --> register
    classDef runtime fill:#eaf2ff,stroke:#3464ad,color:#20314a
    classDef tool fill:#e4f5f1,stroke:#177b69,color:#20314a
    classDef model fill:#fff1dc,stroke:#a56818,color:#20314a
    classDef data fill:#efeafb,stroke:#7652a6,color:#20314a
    classDef source fill:#f0f3f7,stroke:#536273,color:#20314a
    classDef trace fill:#f3f3f3,stroke:#69717a,color:#20314a
    class source source
    class revision data
    class chunk model
    class gate data
    class index source
    class mcp tool
    class answer runtime
    class delta data
    class entailed data
    class synthetic source
    class dataset data
    class register source
```

## Simple search flow

```mermaid
flowchart LR
    INGEST[Ingestion: provenance / durable write / selected-store receipts] -->|background refresh| META[Ordinary collection metadata cache]
    USER[Question + selected collections + chosen VDB] --> SEARCH[Vector search using cached collection metadata]
    META --> SEARCH
    SEARCH --> GLOBAL[Global top 10 across selected collections]
    GLOBAL --> RERANK[Rerank once / retain best 5]
    RERANK --> LLM[Grounded LLM answer]
    LLM --> STREAM[Stream text and linked sources to GUI]
```

## Source rechecks

```mermaid
flowchart TD
    U[Collection: save website scope and optional daily window]
    P[(PostgreSQL source versions and run ledger)]
    T[Run:ai CPU refresh coordinator]
    L[Per-source claim, deadline and fencing]
    A[Bounded full-byte pages and selected assets]
    O[(Immutable observation files and SHA256 manifest)]
    C{Fingerprint matches current published evidence?}
    N[Collection-locked unchanged result: no job or models]
    I[Atomic accepted run, group, job, revisions and outbox]
    R[Redis Streams and native ingestion workers]
    V[Independent ES and optional Milvus writer]
    G[PostgreSQL receipt and publication gate]
    B[Published accepted run advances comparison baseline]
    F[Failed/incomplete scan: retain outcome and prior publication]
    U --> P --> T --> L --> A --> O --> C
    C -->|unchanged| N --> P
    C -->|new, changed or processing identity changed| I --> R --> V --> G --> B --> P
    A -->|unavailable or window exhausted| F --> P
```

## Visual caption location flow

```mermaid
flowchart TD
    ADMIN[Admin / local sign in] --> ROOTS[Versioned PostgreSQL aliases / audit]
    MOUNTS[Operator read-only source mounts] --> ROOTS
    ROOTS -->|enabled aliases only| LOC
    UI[Open or create collection] --> UP[Upload files]
    UI --> LOC[Registered directory or direct document URI]
    UI --> WEB[Website scope and depth]
    LOC --> RUN[PG saved source and leased run]
    WEB --> RUN
    RUN --> CAP[Complete retained acquisition and checksum observations]
    CAP --> SAME{Current published evidence unchanged?}
    SAME -->|yes| OBS[Record unchanged observation; no model work]
    SAME -->|missing only| REVIEW[Record missing review; retain publication]
    SAME -->|new or changed| INTAKE[Atomic PG job / revisions / outbox]
    UP --> INTAKE
    INTAKE --> STREAM[Redis Streams]
    STREAM --> NATIVE[Native PDF / typed / whole-page extraction]
    NATIVE --> OFFICE[Office original structure plus retained visual derivatives]
    NATIVE --> PAGE[One grounded webpage record]
    OFFICE --> PROOF[Immutable stages and independent proof admission]
    PAGE --> PROOF
    PAGE --> OPT{User enabled image captions?}
    OPT -->|yes| QUEUE[PG CAPTION assets and Redis; four attempts with backoff]
    QUEUE --> RESULT[Retained Omni result; isolate failed image]
    RESULT --> FENCE[Verify current page/image associations and opt-in]
    FENCE --> PARENT[Reembed original parent plus labeled captions; one page vector]
    PARENT --> PROOF
    PROOF --> SNAP[Immutable full collection snapshot]
    SNAP --> WRITER[Independent VDB writer]
    WRITER --> ES[Elasticsearch always]
    WRITER --> MV[Optional Milvus GPU_CAGRA]
    ES --> RECEIPT[Verify selected receipts and matching content/vector hashes]
    MV --> RECEIPT
    RECEIPT --> PG[Atomic PostgreSQL current pointer and graph publication]
    PG --> QUERY[Scoped retrieval / MCP]
```
