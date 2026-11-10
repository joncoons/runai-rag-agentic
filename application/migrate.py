"""Explicit, ordered migrations for a fresh or existing application database."""
import asyncio
import hashlib
import os
from pathlib import Path
import asyncpg

ROOT=Path('/app')

def migrations(root=ROOT):
    folders=[root/'runtime/src/nv_ingest_durable/migrations',root/'deployment/collection-control',root/'deployment/ingestion/captioning',root/'deployment/application']
    for folder in folders:
        for path in sorted(folder.glob('*.sql')):
            text=path.read_text().strip()
            # Durable upstream files own an outer transaction; use the ledger's transaction instead.
            if text.startswith('BEGIN;') and text.endswith('COMMIT;'):
                text=text[len('BEGIN;'):-len('COMMIT;')].strip()
            yield str(path.relative_to(root)),hashlib.sha256(path.read_bytes()).hexdigest(),text

async def apply(connection,root=ROOT):
    await connection.execute('CREATE TABLE IF NOT EXISTS public.agentic_schema_migrations (name text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())')
    for name,digest,text in migrations(root):
        async with connection.transaction():
            await connection.execute("SELECT pg_advisory_xact_lock(hashtext('agentic-schema-migrations'))")
            previous=await connection.fetchval('SELECT sha256 FROM public.agentic_schema_migrations WHERE name=$1',name)
            if previous is not None:
                if previous!=digest:raise ValueError('Previously applied migration changed: '+name)
                continue
            await connection.execute(text)
            await connection.execute('INSERT INTO public.agentic_schema_migrations(name,sha256) VALUES($1,$2)',name,digest)

async def main():
    connection=await asyncpg.connect(os.environ['POSTGRES_DSN'])
    try:await apply(connection)
    finally:await connection.close()

if __name__=='__main__':asyncio.run(main())
