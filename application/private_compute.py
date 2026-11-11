"""Killable compute subprocess using the existing qualified native processors."""
import asyncio,json,os,sys
from dataclasses import asdict,replace
from pathlib import Path
from uuid import UUID
import asyncpg
from nv_ingest_durable.contracts import StageEnvelope

async def main():
 payload=json.loads(sys.stdin.read());workspace=UUID(payload['workspace_id']);pool=await asyncpg.create_pool(os.environ['PRIVATE_POSTGRES_DSN'],min_size=1,max_size=4)
 try:
  row=await pool.fetchrow("SELECT * FROM private_control.workspaces WHERE workspace_id=$1 AND state='active' AND expires_at>clock_timestamp()",workspace)
  if not row:raise ValueError('Workspace expired')
  root=Path(os.environ['PRIVATE_ARTIFACT_ROOT'])/str(workspace);stages=root/'stages'
  result_path=Path(payload['result_path'])
  if result_path.resolve().parent!=stages.resolve():raise ValueError('Untrusted result path')
  os.environ['ARTIFACT_ROOT']=str(stages);os.environ['POSTGRES_DSN']=os.environ['PRIVATE_POSTGRES_DSN'];os.environ['REDIS_URL']=os.environ['PRIVATE_REDIS_URL']
  envelope=StageEnvelope.from_stream_fields(payload['envelope'])
  job=await pool.fetchrow('SELECT corpus_namespace FROM public.jobs WHERE job_id=$1',UUID(envelope.job_id))
  if not job or job['corpus_namespace']!=row['collection_id']:raise ValueError('Compute scope differs from job')
  if payload['kind']=='extraction':
   from nv_ingest_durable.pipeline_worker import PipelineWorkerSettings,build_processor
   from nv_ingest_durable.caption_policy import CaptionPolicyProcessor
   from application.typed_extraction import TypedExtractionProcessor
   settings=PipelineWorkerSettings.from_env()
   pdf=CaptionPolicyProcessor(pool,build_processor(settings,pool=pool),build_processor(replace(settings,caption_enabled=False),pool=pool),feature_allowed=True)
   processor=TypedExtractionProcessor(pool,pdf,root=stages,raw_root=root/'raw',reuse_legal=False)
  elif payload['kind']=='caption':
   from nv_ingest_durable.caption_worker import CaptionProcessor
   from nv_ingest_durable.caption_repository import CaptionManifestRepository
   from nv_ingest_durable.postgres import PostgresManifestRepository
   from nv_ingest_durable.pipeline_worker import _model_params,_digest
   from nv_ingest_durable.embedding import RemoteEmbedding
   from application.image_caption_cache import ImageCaptionCache
   params,embed=_model_params()
   import nv_ingest_durable.embedding as module
   embedding=RemoteEmbedding(embed,checkpoint=os.environ['EMBED_CHECKPOINT_DIR'],expected_files=json.loads(Path(module.__file__).with_name('embedding-input-contract.json').read_text()))
   repo=CaptionManifestRepository(PostgresManifestRepository(pool),stages)
   processor=CaptionProcessor(stages,params,embedding,_digest('CAPTION_MODEL_DIGEST'),repo.source_context,ImageCaptionCache(pool,stages))
  else:raise ValueError('Unknown private compute kind')
  result=await processor.process(envelope)
  body={'artifact':asdict(result.artifact),'downstream':[(stream,e.to_dict()) for stream,e in result.downstream]}
  result_path.write_text(json.dumps(body))
 finally:await pool.close()
if __name__=='__main__':
 try:asyncio.run(main())
 except BaseException as error:
  # Never emit raw document content, model response bodies or credentials.
  sys.stderr.write(type(error).__name__+'\n');sys.exit(1)
