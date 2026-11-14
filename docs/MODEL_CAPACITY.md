# Fractional model capacity

The optional Lightning NIM template complements the dedicated Super LLM. An example shared 96 GB Blackwell allocation is:

| Model | Run:ai memory fraction |
|---|---:|
| Nemotron 3.5 Embed | 10% |
| Nemotron 3.5 Parse | 10% |
| Nano Omni captioning | 40% |
| Nemotron 3.5 Lightning | 40% |

Super remains on a separate dedicated GPU. Fractions describe Run:ai reservations/sharing, not four physical MIG devices or a guarantee of fixed compute throughput. Use Run:ai-supported placement and verify the allocated physical GPU; a node selector alone does not select a GPU index. Keep unrelated model services/caches intact.

Use [values-gpu1-packing.yaml](../deployment/helm/values-gpu1-packing.yaml) after the normal model overlay, with installation-specific node, PVC, profile, snapshot and cache identities. The chart continues to default every model off, including Lightning. No model server is deployed by enabling only application routing.

## Lightning checkpoint and runtime cache

Stage the chosen `latest` NIM's profile with its native `download-to-cache` tool and checksum verification, then verify the profile workspace hash. Record both image index/platform digests, profile, exact snapshot/ref and model revision. The generic NVFP4 TP1 non-LoRA profile used for NIM2.1.6 qualification is `920207996e4ec77b396e25f4e360ac80331a67dcae0fec428803f556afe1d47c`; re-resolve it when the image changes. Avoid hardware-specific B300 profiles on SM120.

`models.lightning.checkpointClaim` points to the universal checkpoint/cache PVC. Its root, or optional `checkpointSubPath`, must contain:

```text
ngc/hub/models--nim--nvidia--nemotron-3.5-lightning/
  blobs/
  snapshots/YOUR_MODEL_SNAPSHOT/
  refs/YOUR_MODEL_SNAPSHOT
```

Set `modelSnapshot` to the downloaded snapshot/ref, such as `hf-3db7814` for that qualified profile. The checkpoint PVC is mounted read-only. Blob and snapshot submounts stay read-only at the native SDK's expected paths. An init container copies only the small ref file into writable SDK metadata. Model weights are shared, not copied into the runtime cache.

The runtime PVC must be writable. Choose `cacheSubPath` incorporating GPU SKU/SM, driver, model, image platform digest, workspace/profile, precision and execution settings, including the fraction/context/concurrency. CUDA, Triton, TorchInductor, vLLM and SDK metadata use that directory. Native SDK temporary workspace materialization may remain ephemeral; its weight symlinks resolve to the protected checkpoint bytes. Provision the checkpoint/cache directories and ownership before deployment.

The template requests a 262,144-token context, one sequence and 0.90 vLLM GPU-memory utilization. These are explicit model limits, not truncation instructions. Validate actual long prompts and simultaneous extraction/caption requests before adopting the allocation. Native kernel selection varies with image and GPU: the qualified SM120 path reported Marlin weight-only execution for the NVFP4 checkpoint. Do not infer native FP4 tensor-core execution merely from checkpoint precision. Retain actual startup/inference evidence and measure performance.

## Qualification and recovery

Test readiness and real inference for every model, 2048-dimensional embedding with `truncate: NONE`, representative large document images, image captions, Lightning strict JSON/tool calls/streaming and the supported long context. Sample device/process memory during startup and mixed load, verify image/GPU identities and check restarts/OOM events. Short synthetic tests establish feasibility, not sustained capacity or production quality across all workloads.

Manage workloads through your chosen Run:ai native or Helm AI Application lifecycle. Verify the controls supported by the installed release. Via YAML Deployment workloads may reject the bulk Stopped phase; their Run:ai YAML editor can set `spec.replicas` to zero and back to one. Verify actual pod/GPU release and replacement readiness. The logical desired phase can remain Running while replicas are zero; do not use it alone as lifecycle evidence. If a live workload differs from its saved control-plane manifest, reconcile the known-working native command/configuration before resizing; stale saved commands can otherwise replace a healthy deployment.

Retain exact healthy rollback manifests and versioned runtime caches. Use the supported Run:ai control to stop the new Lightning workload and wait for its GPU allocation to be released before restoring prior fractions. Preserve model bytes and PVCs. A stopped legacy Docker model can remain available for explicit rollback without remaining an untracked GPU consumer.

Model availability and query routing are separate. Lightning/Super can serve local small/medium tiers; a hosted large tier requires separate explicit permission and credential references. Search remains a single retrieval/rerank/streaming flow, and deep research remains a user-selected mode. See [routing configuration](WORKLOAD_ROUTING.md); this capacity overlay does not automatically wire a third target or a coding delegation API into the application.
