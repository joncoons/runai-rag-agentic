"""User opt-in at the ingestion-job boundary; worker configuration is only a ceiling."""
import asyncio
import json
from uuid import UUID


class CaptionPolicyProcessor:
    def __init__(self,pool,enabled,disabled,*,feature_allowed=True):
        self.pool=pool;self.enabled=enabled;self.disabled=disabled;self.feature_allowed=feature_allowed

    async def process(self,envelope):
        row=await self.pool.fetchrow('SELECT metadata FROM public.jobs WHERE job_id=$1',UUID(envelope.job_id))
        if row is None:raise ValueError('Ingestion job does not exist')
        metadata=json.loads(row['metadata']) if isinstance(row['metadata'],str) else dict(row['metadata'] or {})
        option=metadata.get('ingestion_options',{}).get('captions_enabled',False)
        if not isinstance(option,bool):raise ValueError('captions_enabled must be a boolean')
        if option and not self.feature_allowed:raise ValueError('Captioning is unavailable on this deployment')
        selected=self.enabled if option else self.disabled
        if __import__('os').environ.get('INGEST_STREAM_PREFIX')=='rag.agentic':
            from application.typed_extraction import TypedExtractionProcessor
            name='_typed_enabled' if option else '_typed_disabled'
            if not hasattr(self,name):setattr(self,name,TypedExtractionProcessor(self.pool,selected,root=__import__('os').environ['ARTIFACT_ROOT']))
            selected=getattr(self,name)
        return await selected.process(envelope)

    async def process_batch(self,envelopes):
        return tuple(await asyncio.gather(*(self.process(envelope) for envelope in envelopes)))
