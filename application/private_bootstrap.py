"""Explicit private-database schema bootstrap; never target the permanent DB."""
import asyncio,os
from pathlib import Path
from urllib.parse import urlsplit,unquote
import asyncpg

def database(url):return unquote(urlsplit(url).path.strip('/'))
async def install(connection):
 import nv_ingest_durable
 root=Path(__file__).resolve().parents[1]
 for name in ('001_initial.sql','002_coordinator.sql'):
  await connection.execute((Path(nv_ingest_durable.__file__).parent/'migrations'/name).read_text())
 for folder in ('deployment/collection-control','deployment/ingestion/captioning','deployment/application'):
  for path in sorted((root/folder).glob('*.sql')):await connection.execute(path.read_text())
 await connection.execute((root/'deployment/private-analysis/schema.sql').read_text())
async def main():
 private=os.environ['PRIVATE_POSTGRES_DSN']
 if database(private)==database(os.environ['POSTGRES_DSN']):raise ValueError('Private database name must differ from permanent database')
 connection=await asyncpg.connect(private)
 try:
  async with connection.transaction():await install(connection)
 finally:await connection.close()
if __name__=='__main__':asyncio.run(main())
