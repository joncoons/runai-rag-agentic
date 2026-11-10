"""Continuous transactional-outbox relay with low-latency wakeups."""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from .postgres import PostgresManifestRepository
from .redis_streams import RedisStreamsTransport
from .worker import OutboxRelay

logger = logging.getLogger(__name__)


async def run_relay() -> None:
    import asyncpg

    pool = await asyncpg.create_pool(
        os.environ["POSTGRES_DSN"], min_size=1, max_size=2
    )
    transport = RedisStreamsTransport.from_url(os.environ["REDIS_URL"])
    relay = OutboxRelay(PostgresManifestRepository(pool), transport)
    stop = asyncio.Event()
    wake = asyncio.Event()
    loop = asyncio.get_running_loop()
    for event in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(event, stop.set)
    limit = int(os.environ.get("OUTBOX_RELAY_BATCH_SIZE", "500"))
    fallback = float(
        os.environ.get("OUTBOX_RELAY_FALLBACK_SECONDS", "1.0")
    )
    listener = await pool.acquire()

    def notified(*_args) -> None:
        wake.set()

    await listener.add_listener("durable_outbox", notified)
    leader = False
    try:
        while not stop.is_set():
            if not leader:
                leader = bool(
                    await listener.fetchval(
                        "SELECT pg_try_advisory_lock("
                        "hashtext('nv_ingest_durable_outbox_relay'))"
                    )
                )
                if not leader:
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=fallback)
                    except TimeoutError:
                        pass
                    continue
            published = await relay.publish_once(limit=limit)
            if published >= limit:
                continue
            wake.clear()
            stop_wait = asyncio.create_task(stop.wait())
            wake_wait = asyncio.create_task(wake.wait())
            done, pending = await asyncio.wait(
                {stop_wait, wake_wait},
                timeout=fallback,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            if stop_wait in done and stop_wait.result():
                break
    finally:
        if leader:
            await listener.execute(
                "SELECT pg_advisory_unlock("
                "hashtext('nv_ingest_durable_outbox_relay'))"
            )
        await listener.remove_listener("durable_outbox", notified)
        await pool.release(listener)
        await transport.client.aclose()
        await pool.close()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_relay())
