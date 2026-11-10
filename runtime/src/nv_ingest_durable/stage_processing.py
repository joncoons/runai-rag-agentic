"""Concrete durable processors and deterministic stage handoffs."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Mapping
from uuid import NAMESPACE_URL, uuid5

from .contracts import Stage, StageEnvelope, derive_idempotency_key
from .elasticsearch_writer import EsReadyCheckpointStager


def result_records(value: Any) -> list[Mapping[str, Any]]:
    """Normalize 26.5 DataFrame/list/wrapped results without importing pandas."""
    if isinstance(value, Mapping) and "result_data" in value:
        value = value["result_data"]
    if hasattr(value, "to_json"):
        value = json.loads(value.to_json(orient="records"))
    elif hasattr(value, "to_dict"):
        value = value.to_dict(orient="records")
    if value is None:
        return []
    if not isinstance(value, list) or any(
        not isinstance(row, Mapping) for row in value
    ):
        raise TypeError("stage result must be a DataFrame or a list of record mappings")
    return list(value)


def downstream_envelope(
    source: StageEnvelope,
    *,
    stage: Stage,
    artifact_uri: str,
    config_digest: str,
    model_digest: str,
) -> StageEnvelope:
    key = derive_idempotency_key(
        source_content_hash=source.document_id,
        logical_unit_id=f"{source.document_id}:{stage.value}",
        stage=stage,
        pipeline_version=source.pipeline_version,
        stage_config={"config_digest": config_digest},
        model_digest=model_digest,
    )
    message_id = str(uuid5(NAMESPACE_URL, f"{source.job_id}:{key}"))
    return StageEnvelope(
        job_id=source.job_id,
        document_id=source.document_id,
        stage=stage,
        artifact_uri=artifact_uri,
        idempotency_key=key,
        pipeline_version=source.pipeline_version,
        config_digest=config_digest,
        model_digest=model_digest,
        message_id=message_id,
        priority=source.priority,
        trace_id=source.trace_id,
    )


@dataclass(slots=True)
class FusedCheckpointStager:
    """Terminate a fused extract/caption/embed graph at the INDEX boundary."""

    delegate: EsReadyCheckpointStager
    embedding_config_digest: str
    embedding_model_digest: str

    async def stage(self, envelope: StageEnvelope, graph_result: Any):
        embedding = downstream_envelope(
            envelope,
            stage=Stage.EMBEDDING,
            artifact_uri=envelope.artifact_uri,
            config_digest=self.embedding_config_digest,
            model_digest=self.embedding_model_digest,
        )
        return await self.delegate.stage(embedding, result_records(graph_result))
