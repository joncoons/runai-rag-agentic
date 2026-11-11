"""Isolated private processing context; never use permanent pools or artifacts."""
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from .context import Context
from .settings import Settings


def private_settings(settings=None):
 settings=settings or Settings.load()
 root=Path(os.environ['PRIVATE_ARTIFACT_ROOT']).resolve()
 if root==settings.root.resolve() or settings.root.resolve() in root.parents or root in settings.root.resolve().parents:
  raise ValueError('Private artifacts must be outside permanent artifact storage')
 dsn=os.environ['PRIVATE_POSTGRES_DSN'];redis=os.environ['PRIVATE_REDIS_URL']
 from .private_bootstrap import database
 from urllib.parse import urlsplit
 if database(dsn)==database(settings.dsn) or (urlsplit(redis).hostname,urlsplit(redis).port)==(urlsplit(settings.redis_url).hostname,urlsplit(settings.redis_url).port):raise ValueError('Private database and Redis instance must be separate')
 if dsn==settings.dsn or redis==settings.redis_url:raise ValueError('Private processing needs isolated database and queue endpoints')
 return replace(settings,dsn=dsn,redis_url=redis,root=root,es_user=os.environ['PRIVATE_ES_USER'],es_password=os.environ['PRIVATE_ES_PASSWORD'])

async def open_private(settings=None):
 ctx=await Context(private_settings(settings)).open()
 from collection_control.admission_repository import ProvenanceRepository
 ctx.control=ProvenanceRepository(ctx.pool,handle_prefix='rag_analysis',incremental=False)
 from collection_control.writer import GenerationWriter
 ctx.writer=GenerationWriter(ctx.control,ctx.settings.snapshots,ctx.stores)
 return ctx


def scoped_context(base,workspace):
 from collection_control.writer import GenerationWriter
 root=base.settings.root/str(workspace['workspace_id'])
 settings=replace(base.settings,root=root)
 return SimpleNamespace(settings=settings,pool=base.pool,http=base.http,es_http=base.es_http,
  stores=base.stores,control=base.control,writer=GenerationWriter(base.control,settings.snapshots,base.stores))
