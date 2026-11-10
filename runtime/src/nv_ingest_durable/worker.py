"""Claim, heartbeat, execute, commit, retry, and acknowledge protocol."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from .contracts import ClaimedMessage, StageResult, WorkerOutcome
from .ports import ArtifactStore, ManifestRepository, StageProcessor, StreamTransport


@dataclass(slots=True)
class _LeasedMessage:
    position: int
    message: ClaimedMessage
    lease_token: str
    stop_heartbeat: asyncio.Event
    lease_lost: asyncio.Event
    heartbeat: asyncio.Task


@dataclass(slots=True)
class DurableStageWorker:
    owner: str
    manifests: ManifestRepository
    artifacts: ArtifactStore
    processor: StageProcessor
    transport: StreamTransport
    lease_ttl: timedelta = timedelta(minutes=10)
    heartbeat_interval: float = 30.0
    max_attempts: int = 5
    deadletter_stream: str = "ingest.deadletter"

    async def _heartbeat(
        self,
        message: ClaimedMessage,
        lease_token: str,
        stop: asyncio.Event,
        lease_lost: asyncio.Event,
    ) -> None:
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.heartbeat_interval)
                return
            except TimeoutError:
                pass
            try:
                renewed = await self.manifests.renew_lease(
                    message.envelope,
                    lease_token=lease_token,
                    ttl=self.lease_ttl,
                )
                if not renewed:
                    lease_lost.set()
                    return
                await self.transport.touch(message, consumer=self.owner)
            except Exception:
                lease_lost.set()
                return

    async def _lease(
        self, position: int, message: ClaimedMessage
    ) -> tuple[_LeasedMessage | None, WorkerOutcome | None]:
        envelope = message.envelope
        committed = await self.manifests.get_committed(envelope.idempotency_key)
        if committed is not None:
            await self.transport.acknowledge(message)
            return None, WorkerOutcome.ALREADY_COMMITTED

        lease_token = await self.manifests.acquire_lease(
            envelope, owner=self.owner, ttl=self.lease_ttl
        )
        if lease_token is None:
            return None, WorkerOutcome.LEASE_BUSY

        stop_heartbeat = asyncio.Event()
        lease_lost = asyncio.Event()
        heartbeat = asyncio.create_task(
            self._heartbeat(message, lease_token, stop_heartbeat, lease_lost)
        )
        return (
            _LeasedMessage(
                position, message, lease_token, stop_heartbeat, lease_lost, heartbeat
            ),
            None,
        )

    async def _record_failure(
        self, leased: _LeasedMessage, error: Exception
    ) -> WorkerOutcome:
        attempts = await self.manifests.record_failure(
            leased.message.envelope,
            lease_token=leased.lease_token,
            error=error,
        )
        if attempts >= self.max_attempts:
            await self.manifests.deadletter(
                leased.message, error=error, target_stream=self.deadletter_stream
            )
            return WorkerOutcome.DEADLETTERED
        return WorkerOutcome.FAILED

    async def _commit(
        self, leased: _LeasedMessage, result: StageResult
    ) -> WorkerOutcome:
        try:
            if leased.lease_lost.is_set():
                raise RuntimeError("stage lease was lost during processing")
            artifact = await self.artifacts.finalize(result.artifact)
            await self.manifests.commit_stage(
                leased.message.envelope,
                lease_token=leased.lease_token,
                artifact=artifact,
                downstream=result.downstream,
            )
        except Exception as error:
            return await self._record_failure(leased, error)
        return WorkerOutcome.COMMITTED

    async def handle_batch(
        self, messages: tuple[ClaimedMessage, ...]
    ) -> tuple[WorkerOutcome, ...]:
        if not messages:
            return ()
        outcomes: list[WorkerOutcome | None] = [None] * len(messages)
        leased: list[_LeasedMessage] = []
        for position, message in enumerate(messages):
            claim, outcome = await self._lease(position, message)
            if claim is not None:
                leased.append(claim)
            else:
                outcomes[position] = outcome

        cancelled = False
        if leased:
            try:
                process_batch = getattr(self.processor, "process_batch", None)
                if callable(process_batch) and len(leased) > 1:
                    results = tuple(
                        await process_batch(
                            tuple(item.message.envelope for item in leased)
                        )
                    )
                else:
                    results = tuple(
                        await asyncio.gather(
                            *(
                                self.processor.process(item.message.envelope)
                                for item in leased
                            )
                        )
                    )
                if len(results) != len(leased):
                    raise RuntimeError(
                        "batch processor returned a different result count"
                    )
            except asyncio.CancelledError:
                cancelled = True
                raise
            except Exception as error:
                for item in leased:
                    outcomes[item.position] = await self._record_failure(item, error)
            else:
                committed = await asyncio.gather(
                    *(
                        self._commit(item, result)
                        for item, result in zip(leased, results)
                    )
                )
                for item, outcome in zip(leased, committed):
                    outcomes[item.position] = outcome
            finally:
                for item in leased:
                    item.stop_heartbeat.set()
                await asyncio.gather(*(item.heartbeat for item in leased))
                await asyncio.gather(
                    *(
                        self.manifests.release_lease(
                            item.message.envelope, lease_token=item.lease_token
                        )
                        for item in leased
                    )
                )

        if not cancelled:
            for message, outcome in zip(messages, outcomes):
                if outcome in {
                    WorkerOutcome.COMMITTED,
                    WorkerOutcome.DEADLETTERED,
                }:
                    await self.transport.acknowledge(message)
        if any(outcome is None for outcome in outcomes):
            raise RuntimeError("worker batch did not produce every outcome")
        return tuple(outcomes)  # type: ignore[arg-type]

    async def handle(self, message: ClaimedMessage) -> WorkerOutcome:
        return (await self.handle_batch((message,)))[0]



@dataclass(slots=True)
class OutboxRelay:
    manifests: ManifestRepository
    transport: StreamTransport

    async def publish_once(self, *, limit: int = 100) -> int:
        events = await self.manifests.fetch_unpublished_outbox(limit=limit)
        published = 0
        for event in events:
            stream_id = await self.transport.publish(event.stream, event.envelope)
            await self.manifests.mark_outbox_published(event.event_id, stream_id)
            published += 1
        return published
