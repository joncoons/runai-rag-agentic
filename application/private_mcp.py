"""Owned private document evidence over the existing MCP protocol."""
from contextlib import asynccontextmanager
import os
from typing import Any
from mcp import types
from mcp.server.mcpserver import Context,MCPServer
from collection_control.repository import Conflict
from .private_context import open_private,scoped_context
from .private_workspaces import PrivateWorkspaces
from .private_scope import verify
from .query_catalog import QueryCatalog
from .query import RAGQuery


def create_server():
 @asynccontextmanager
 async def lifespan(server):
  base=await open_private()
  try:yield {'private':base}
  finally:await base.close()
 server=MCPServer('Private document evidence',version='1.0.0',lifespan=lifespan)
 async def scoped(ctx,collections):
  request=ctx.request_context.request
  if request is None:raise Conflict('Private evidence requires controller scope')
  workspace,owner=verify(request.headers.get('x-analysis-scope',''))
  base=ctx.request_context.lifespan_context['private']
  row=await PrivateWorkspaces(base).get(owner,workspace)
  if not row or collections!=[row['collection_id']]:raise Conflict('Private evidence scope is unavailable')
  context=scoped_context(base,row)
  catalog=QueryCatalog(base.pool,snapshots=context.settings.snapshots,allowed_collection_ids=collections)
  await catalog.refresh()
  return RAGQuery(context.settings,base.pool,base.http,catalog=catalog),base,row
 readonly=types.ToolAnnotations(read_only_hint=True,destructive_hint=False,idempotent_hint=True,open_world_hint=False)
 @server.tool(annotations=readonly,structured_output=True)
 async def query_evidence(query:str,collection_ids:list[str],ctx:Context,backend:str='elasticsearch',include_legal:bool=False)->dict[str,Any]:
  service,base,row=await scoped(ctx,collection_ids)
  if backend!='elasticsearch':raise Conflict('Private analysis uses its isolated Elasticsearch projection')
  result=await service.query_evidence(query,collection_ids,backend,include_legal=True)
  verify(ctx.request_context.request.headers.get('x-analysis-scope',''))
  if not await PrivateWorkspaces(base).get(row['owner_id'],row['workspace_id']):raise Conflict('Private analysis expired')
  return result
 @server.tool(annotations=readonly,structured_output=True)
 async def get_source(record_id:str,collection_ids:list[str],ctx:Context,backend:str='elasticsearch')->dict[str,Any]:
  service,base,row=await scoped(ctx,collection_ids)
  result=await service.get_source(record_id,collection_ids,backend)
  verify(ctx.request_context.request.headers.get('x-analysis-scope',''))
  if not await PrivateWorkspaces(base).get(row['owner_id'],row['workspace_id']):raise Conflict('Private analysis expired')
  return result
 return server

if __name__=='__main__':
 create_server().run(transport='streamable-http',host='0.0.0.0',port=int(os.environ.get('PRIVATE_MCP_PORT','8012')),streamable_http_path='/mcp',stateless_http=True,json_response=True)
