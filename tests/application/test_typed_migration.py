"""Migration accepts previously valid long URLs and preserves exact identities."""
import os
from pathlib import Path
import unittest
from uuid import uuid4
import asyncpg

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'requires disposable qualification database')
class TypedMigrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_long_url_migration_and_identical_bytes_distinct_urls(self):
        c=await asyncpg.connect(os.environ['COLLECTION_TEST_DSN'])
        try:
            identity=await c.fetchrow('SELECT current_database() db,current_user usr')
            self.assertEqual((identity['db'],identity['usr']),('qualification','qualification'))
            transaction=c.transaction();await transaction.start()
            try:
                await c.execute('TRUNCATE public.jobs CASCADE; DROP INDEX IF EXISTS public.documents_job_uri_sha_digest_key; DROP INDEX IF EXISTS public.documents_job_uri_digest_key; ALTER TABLE public.documents ADD CONSTRAINT documents_job_id_source_digest_key UNIQUE(job_id,source_digest)')
                job=uuid4();uri='https://qualification.invalid/'+''.join(uuid4().hex for _ in range(160))
                await c.execute("INSERT INTO public.jobs(job_id,corpus_namespace,status,pipeline_version) VALUES($1,'qualification','accepted','v1')",job)
                await c.execute("INSERT INTO public.documents(document_id,job_id,source_uri,source_digest,status) VALUES('long-url', $1,$2,$3,'accepted')",job,uri,'sha256:'+'a'*64)
                await c.execute(Path('deployment/application/002_typed_sources.sql').read_text())
                self.assertEqual(await c.fetchval("SELECT source_uri FROM public.documents WHERE document_id='long-url'"),uri)
                await c.execute("INSERT INTO public.documents(document_id,job_id,source_uri,source_digest,status) VALUES('other-url',$1,$2,$3,'accepted')",job,uri+'different','sha256:'+'a'*64)
                self.assertEqual(await c.fetchval('SELECT count(*) FROM public.documents WHERE job_id=$1',job),2)
                self.assertEqual(await c.fetchval("SELECT count(*) FROM pg_constraint WHERE conrelid='public.documents'::regclass AND contype='p'"),1)
            finally:await transaction.rollback()
        finally:await c.close()
