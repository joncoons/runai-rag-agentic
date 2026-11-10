"""Bounded stage-worker polling and abandoned-message recovery loops."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging

from .worker import DurableStageWorker


logger = logging.getLogger(__name__)

@dataclass(slots=True)
class StageWorkerService:
    stream: str
    group: str
    consumer: str
    worker: DurableStageWorker
    max_concurrency: int = 4
    read_count: int = 4
    block_ms: int = 5000
    reclaim_idle_ms: int = 900_000
    reclaim_count: int = 20
    microbatch_size: int = 1
    microbatch_wait_ms: int = 0
    _reclaim_cursor: str = field(default="0-0", init=False)

    def __post_init__(self) -> None:
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        if not 1 <= self.microbatch_size <= self.max_concurrency:
            raise ValueError("microbatch_size must be within worker concurrency")
        if self.microbatch_size > 1 and self.microbatch_wait_ms < 1:
            raise ValueError("microbatch_wait_ms must be positive for batching")

    async def bootstrap(self) -> None:
        await self.worker.transport.ensure_group(self.stream, self.group)

    async def _handle_many(self, messages) -> list:
        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def bounded(message):
            async with semaphore:
                return await self.worker.handle(message)

        return await asyncio.gather(*(bounded(message) for message in messages))

    async def poll_once(self) -> list:
        messages = await self.worker.transport.read(
            self.stream,
            self.group,
            self.consumer,
            count=self.read_count,
            block_ms=self.block_ms,
        )
        return await self._handle_many(messages)

    async def reclaim_once(self) -> list:
        cursor, messages = await self.worker.transport.reclaim(
            self.stream,
            self.group,
            self.consumer,
            min_idle_ms=self.reclaim_idle_ms,
            start_id=self._reclaim_cursor,
            count=self.reclaim_count,
        )
        self._reclaim_cursor = cursor
        return await self._handle_many(messages)

    async def _claim_one(self, *, reclaim: bool, block_ms: int | None = None):
        if reclaim:
            cursor, messages = await self.worker.transport.reclaim(
                self.stream,
                self.group,
                self.consumer,
                min_idle_ms=self.reclaim_idle_ms,
                start_id=self._reclaim_cursor,
                count=1,
            )
            self._reclaim_cursor = cursor
            if messages:
                return messages[0]
        messages = await self.worker.transport.read(
            self.stream,
            self.group,
            self.consumer,
            count=1,
            block_ms=self.block_ms if block_ms is None else block_ms,
        )
        return messages[0] if messages else None

    async def _collect_batch(self, first, slots: asyncio.Semaphore):
        messages = [first]
        if self.microbatch_size == 1:
            return tuple(messages)
        deadline = (
            asyncio.get_running_loop().time()
            + self.microbatch_wait_ms / 1000
        )
        while len(messages) < self.microbatch_size:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(slots.acquire(), timeout=remaining)
            except TimeoutError:
                break
            try:
                message = await asyncio.wait_for(
                    self._claim_one(
                        reclaim=False,
                        block_ms=max(1, int(remaining * 1000)),
                    ),
                    timeout=remaining,
                )
            except TimeoutError:
                slots.release()
                break
            except BaseException:
                slots.release()
                raise
            if message is None:
                slots.release()
                break
            messages.append(message)
        return tuple(messages)

    async def run(self, stop: asyncio.Event, *, reclaim_every: int = 12) -> None:
        await self.bootstrap()
        slots = asyncio.Semaphore(self.max_concurrency)
        in_flight: set[asyncio.Task] = set()
        failures: list[BaseException] = []
        claims = 0

        async def handle(messages) -> None:
            try:
                if len(messages) == 1:
                    await self.worker.handle(messages[0])
                else:
                    await self.worker.handle_batch(messages)
            except BaseException as error:
                failures.append(error)
                stop.set()
                logger.exception(
                    "stage worker task failed stream=%s group=%s",
                    self.stream,
                    self.group,
                )
            finally:
                for _ in messages:
                    slots.release()

        def completed(task: asyncio.Task) -> None:
            in_flight.discard(task)
            # Retrieve the result even though handle captures operational
            # failures, so cancellation never produces an unobserved warning.
            try:
                task.result()
            except asyncio.CancelledError:
                pass

        while not stop.is_set():
            await slots.acquire()
            if stop.is_set():
                slots.release()
                break
            try:
                message = await self._claim_one(
                    reclaim=bool(claims and claims % reclaim_every == 0)
                )
            except BaseException:
                slots.release()
                raise
            if message is None:
                slots.release()
                continue
            messages = await self._collect_batch(message, slots)
            claims += len(messages)
            task = asyncio.create_task(handle(messages))
            in_flight.add(task)
            task.add_done_callback(completed)

        if in_flight:
            await asyncio.gather(*tuple(in_flight), return_exceptions=True)
        if failures:
            raise failures[0]
