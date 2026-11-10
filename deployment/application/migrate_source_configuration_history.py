"""Apply only this release's additive history table; never replay unrelated DDL."""
import asyncio,os
from pathlib import Path
import asyncpg

async def apply(connection):
    async with connection.transaction():
        await connection.execute("SET LOCAL lock_timeout='5s'")
        await connection.execute("SET LOCAL statement_timeout='30s'")
        await connection.execute(Path(__file__).with_name('012_source_configuration_history.sql').read_text())

async def main():
    connection=await asyncpg.connect(os.environ['POSTGRES_DSN'])
    try:await apply(connection)
    finally:await connection.close()

if __name__=='__main__':asyncio.run(main())
