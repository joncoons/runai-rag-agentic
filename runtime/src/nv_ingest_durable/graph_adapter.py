"""NeMo 26.5 graph boundary without transport or persistence side effects."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Protocol
from urllib.parse import unquote, urlparse

from .contracts import PendingArtifact, StageEnvelope, StageResult


class GraphRunner(Protocol):
    async def run(
        self,
        *,
        graph_name: str,
        artifact_uri: str,
        configuration: Mapping[str, Any],
    ) -> Any: ...


class ResultStager(Protocol):
    async def stage(
        self, envelope: StageEnvelope, graph_result: Any
    ) -> tuple[PendingArtifact, tuple[tuple[str, StageEnvelope], ...]]: ...


ConfigurationLoader = Callable[[StageEnvelope], Awaitable[Mapping[str, Any]]]
InputResolver = Callable[[str], Any]
ExecutorFactory = Callable[[Any], Any]


def local_file_input(artifact_uri: str) -> str:
    """Resolve a durable file URI into an executor input path.

    Object-store deployments inject a materializer that downloads or mounts the
    immutable artifact before graph execution.
    """

    parsed = urlparse(artifact_uri)
    if parsed.scheme != "file":
        raise ValueError(
            "default NeMo graph input requires file://; inject an object-store "
            f"materializer for {artifact_uri!r}"
        )
    return str(Path(unquote(parsed.path)))


@dataclass(slots=True)
class RegistryGraphRunner:
    """Build and synchronously execute a named 26.5 graph blueprint."""

    registry: Any
    executor_factory: ExecutorFactory
    input_resolver: InputResolver = local_file_input

    @classmethod
    def from_nemo(
        cls,
        *,
        mode: str = "inprocess",
        registry: Any = None,
        input_resolver: InputResolver = local_file_input,
        executor_options: Mapping[str, Any] | None = None,
    ) -> "RegistryGraphRunner":
        from nemo_retriever.graph import InprocessExecutor, RayDataExecutor
        from nemo_retriever.graph.graph_pipeline_registry import default_registry

        options = dict(executor_options or {})
        if mode == "inprocess":
            factory = lambda graph: InprocessExecutor(graph, **options)
        elif mode == "ray":
            factory = lambda graph: RayDataExecutor(graph, **options)
        else:
            raise ValueError("mode must be 'inprocess' or 'ray'")
        selected_registry = registry or default_registry
        if registry is None and not selected_registry.list_names():
            raise RuntimeError(
                "the 26.5 default graph registry is empty; import/register the "
                "deployment blueprint or inject an explicit registry"
            )
        return cls(
            registry=selected_registry,
            executor_factory=factory,
            input_resolver=input_resolver,
        )

    def _build_executor(self, graph_name: str, overrides: Mapping[str, Any]):
        if overrides:
            graph = self.registry.build_with_overrides(graph_name, dict(overrides))
        else:
            graph = self.registry.build(graph_name)
        return self.executor_factory(graph)

    async def run(
        self,
        *,
        graph_name: str,
        artifact_uri: str,
        configuration: Mapping[str, Any],
    ) -> Any:
        overrides = configuration.get("node_overrides") or {}
        executor = self._build_executor(graph_name, overrides)
        graph_input = self.input_resolver(artifact_uri)
        return await asyncio.to_thread(executor.ingest, graph_input)


    async def run_batch(
        self,
        *,
        graph_name: str,
        artifact_uris: tuple[str, ...],
        configuration: Mapping[str, Any],
    ) -> tuple[Any, ...]:
        if not artifact_uris:
            return ()
        overrides = configuration.get("node_overrides") or {}
        executor = self._build_executor(graph_name, overrides)

        def execute():
            import pandas as pd

            frames = [self.input_resolver(uri) for uri in artifact_uris]
            if any(not hasattr(frame, "iloc") for frame in frames):
                raise TypeError("batched graph inputs must be DataFrame-like")
            sizes = [len(frame) for frame in frames]
            combined = pd.concat(frames, ignore_index=True)
            result = executor.ingest(combined)
            if hasattr(result, "iloc"):
                if len(result) != sum(sizes):
                    raise RuntimeError("batched graph changed row cardinality")
                split = []
                offset = 0
                for size in sizes:
                    split.append(result.iloc[offset : offset + size].copy())
                    offset += size
                return tuple(split)
            if isinstance(result, list):
                if len(result) != sum(sizes):
                    raise RuntimeError("batched graph changed row cardinality")
                split = []
                offset = 0
                for size in sizes:
                    split.append(result[offset : offset + size])
                    offset += size
                return tuple(split)
            raise TypeError("batched graph output must be DataFrame-like or a list")

        return await asyncio.to_thread(execute)

@dataclass(slots=True)
class NemoGraphStageProcessor:
    """Run one named subgraph inside the outer durable worker boundary.

    This class has no Redis, PostgreSQL, or Elasticsearch imports by design.
    Graph execution may repeat; only the outer worker can commit or acknowledge.
    """

    graph_name: str
    runner: GraphRunner
    configuration_loader: ConfigurationLoader
    stager: ResultStager
    batch_enabled: bool = False

    async def process(self, envelope: StageEnvelope) -> StageResult:
        configuration = await self.configuration_loader(envelope)
        result = await self.runner.run(
            graph_name=self.graph_name,
            artifact_uri=envelope.artifact_uri,
            configuration=configuration,
        )
        artifact, downstream = await self.stager.stage(envelope, result)
        return StageResult(artifact=artifact, downstream=downstream)

    async def process_batch(
        self, envelopes: tuple[StageEnvelope, ...]
    ) -> tuple[StageResult, ...]:
        if not self.batch_enabled or len(envelopes) < 2:
            return tuple(await asyncio.gather(*(self.process(item) for item in envelopes)))
        configurations = tuple(
            await asyncio.gather(
                *(self.configuration_loader(item) for item in envelopes)
            )
        )
        if any(item != configurations[0] for item in configurations[1:]):
            raise ValueError("batched graph envelopes require identical configuration")
        run_batch = getattr(self.runner, "run_batch", None)
        if not callable(run_batch):
            raise TypeError("graph runner does not support batch execution")
        graph_results = tuple(
            await run_batch(
                graph_name=self.graph_name,
                artifact_uris=tuple(item.artifact_uri for item in envelopes),
                configuration=configurations[0],
            )
        )
        if len(graph_results) != len(envelopes):
            raise RuntimeError("graph batch returned a different result count")
        staged = await asyncio.gather(
            *(
                self.stager.stage(envelope, graph_result)
                for envelope, graph_result in zip(envelopes, graph_results)
            )
        )
        return tuple(
            StageResult(artifact=artifact, downstream=downstream)
            for artifact, downstream in staged
        )
