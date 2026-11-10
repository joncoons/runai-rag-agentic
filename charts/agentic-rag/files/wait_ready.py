"""Fence startup on this image's schema and the selected tokenizer contract."""
import asyncio,hashlib,json,os
from pathlib import Path
import asyncpg
from application.migrate import migrations
from application.legal_text_budget import load_budget

async def main():
    expected={name:digest for name,digest,_ in migrations(Path('/app'))}
    checkpoint=os.environ['EXPECTED_EMBED_CHECKPOINT_SHA256']
    max_characters=int(os.environ['EXPECTED_EMBED_MAX_CHARACTERS'])
    for attempt in range(180):
        connection=None
        try:
            connection=await asyncpg.connect(os.environ['POSTGRES_DSN'],timeout=5)
            rows=await connection.fetch('SELECT name,sha256 FROM public.agentic_schema_migrations')
            actual={row['name']:row['sha256'] for row in rows}
            if not all(actual.get(name)==digest for name,digest in expected.items()):raise ValueError('Schema is not ready')
            # A concurrent preparation Job can replace a valid older contract.
            # Never keep its cached tokenizer while waiting for the selected revision.
            load_budget.cache_clear()
            budget=load_budget()
            if budget.checkpoint!=checkpoint or budget.max_characters!=max_characters:raise ValueError('Selected tokenizer contract is not ready')
            load_budget(budget.identity)  # Historical replay must be ready too.
            contract=Path(os.environ.get('LEGAL_INPUT_CONTRACT','/app/config/legal-input-contract.json'))
            files=json.loads(contract.read_text())['files']
            embedding=Path(os.environ.get('EMBED_INPUT_CONTRACT','/app/config/embedding-input-contract.json'))
            if json.loads(embedding.read_text())!=files:raise ValueError('Embedding and legal tokenizer contracts differ')
            engine=os.environ.get('EMBED_CHECKPOINT_DIR')
            if engine and any(hashlib.sha256((Path(engine)/name).read_bytes()).hexdigest()!=digest for name,digest in files.items()):raise ValueError('Mounted embedding inputs differ from selected contract')
            return
        except Exception:
            if attempt==179:raise RuntimeError('Migration/tokenizer readiness deadline exceeded') from None
            await asyncio.sleep(5)
        finally:
            if connection is not None:await connection.close()

if __name__=='__main__':asyncio.run(main())
