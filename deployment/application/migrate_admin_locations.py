"""Apply only the additive session/alias tables for this release."""
import asyncio,os
from pathlib import Path
import asyncpg
async def apply(connection):
    async with connection.transaction():
        await connection.execute("SET LOCAL lock_timeout='5s'")
        await connection.execute("SET LOCAL statement_timeout='30s'")
        for name in ('013_local_sessions.sql','014_location_registry.sql'):
            await connection.execute(Path(__file__).with_name(name).read_text())
async def main():
    connection=await asyncpg.connect(os.environ['POSTGRES_DSN'])
    try:await apply(connection)
    finally:await connection.close()
if __name__=='__main__':asyncio.run(main())
