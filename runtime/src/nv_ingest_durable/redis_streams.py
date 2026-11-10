"""Redis Streams transport adapter.

The optional redis dependency is imported at construction time so the domain
and graph integration modules remain importable without infrastructure clients.
"""

from __future__ import annotations

from typing import Any

from .contracts import ClaimedMessage, StageEnvelope


def _text(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


class RedisStreamsTransport:
    def __init__(self, client: Any) -> None:
        self.client = client

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> "RedisStreamsTransport":
        from redis.asyncio import Redis

        return cls(Redis.from_url(url, decode_responses=False, **kwargs))

    async def ensure_group(self, stream: str, group: str) -> None:
        try:
            await self.client.xgroup_create(stream, group, id="0-0", mkstream=True)
        except Exception as error:
            if "BUSYGROUP" not in str(error):
                raise

    async def publish(self, stream: str, envelope: StageEnvelope) -> str:
        stream_id = await self.client.xadd(stream, envelope.to_stream_fields())
        return _text(stream_id)

    async def read(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int = 1,
        block_ms: int = 5000,
    ) -> list[ClaimedMessage]:
        try:
            response = await self.client.xreadgroup(
                group,
                consumer,
                streams={stream: ">"},
                count=count,
                block=block_ms,
            )
        except Exception as error:
            # An idle blocking read timing out means there were no entries.
            if (
                error.__class__.__name__ != "TimeoutError"
                or not error.__class__.__module__.startswith("redis")
            ):
                raise
            return []
        return self._claimed(response, group)

    async def reclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        min_idle_ms: int,
        start_id: str = "0-0",
        count: int = 100,
    ) -> tuple[str, list[ClaimedMessage]]:
        response = await self.client.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_ms,
            start_id=start_id,
            count=count,
        )
        next_id = _text(response[0])
        entries = response[1]
        claimed = [
            ClaimedMessage(
                stream=stream,
                group=group,
                redis_id=_text(message_id),
                envelope=StageEnvelope.from_stream_fields(fields),
            )
            for message_id, fields in entries
        ]
        return next_id, claimed

    async def acknowledge(self, message: ClaimedMessage) -> None:
        await self.client.xack(message.stream, message.group, message.redis_id)

    async def touch(self, message: ClaimedMessage, *, consumer: str) -> None:
        """Reset pending-entry idle time while a long stage is still alive."""

        await self.client.xclaim(
            message.stream, message.group, consumer, min_idle_time=0,
            message_ids=[message.redis_id], justid=True
        )

    @staticmethod
    def _claimed(response: Any, group: str) -> list[ClaimedMessage]:
        claimed: list[ClaimedMessage] = []
        for stream, entries in response or []:
            stream_name = _text(stream)
            for message_id, fields in entries:
                claimed.append(
                    ClaimedMessage(
                        stream=stream_name,
                        group=group,
                        redis_id=_text(message_id),
                        envelope=StageEnvelope.from_stream_fields(fields),
                    )
                )
        return claimed
