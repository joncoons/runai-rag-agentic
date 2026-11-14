"""Offline Helm contract checks; no Kubernetes resources or GPU work are created."""
import copy,hashlib,json,shutil,subprocess,tempfile,unittest
from pathlib import Path
try:import yaml
except ImportError:yaml=None
ROOT=Path(__file__).resolve().parents[2]

@unittest.skipUnless(shutil.which('helm') and yaml,'Requires Helm 3 and PyYAML')
class HelmContracts(unittest.TestCase):
 def values(self):
  v=yaml.safe_load((ROOT/'charts/agentic-rag/values.yaml').read_text());v['runai']['projectName']='validation';v['application']['image']='registry.example/agentic-rag@sha256:'+'a'*64
  for key in v['bindings']:
   v['bindings'][key]='http://models.example/v1/test' if 'ENDPOINT' in key or key.endswith('_URL') else 'sha256:'+'a'*64 if 'PLATFORM_IMAGE_DIGEST' in key else 'a'*64
  return v
 def render(self,values,chart='agentic-rag',success=True):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'values.yaml';p.write_text(yaml.safe_dump(values))
   out=subprocess.run(['helm','template','rag',str(ROOT/'charts'/chart),'--namespace','validation','-f',str(p)],text=True,capture_output=True)
  if not success:self.assertNotEqual(out.returncode,0);return out.stderr
  self.assertEqual(out.returncode,0,out.stderr)
  return [d for d in yaml.safe_load_all(out.stdout) if d]
 def test_web_research_is_opt_in_and_only_ordinary_research_has_provider_secret(self):
  v=self.values();v['privateAnalysis']['enabled']=True;v['research']={'webSearch':{'enabled':True,'secretName':'web-provider','secretKey':'TAVILY_API_KEY'}}
  docs=self.render(v);roles={d['metadata']['name']:d for d in docs if d['kind']=='Deployment'}
  for name,d in roles.items():
   env={e['name']:e for e in d['spec']['template']['spec']['containers'][0].get('env',[])}
   if name in ('rag-api','rag-research'):self.assertEqual(env['RESEARCH_WEB_SEARCH_ENABLED']['value'],'true')
   else:self.assertNotIn('TAVILY_API_KEY',env)
   if name=='rag-research':self.assertEqual(env['TAVILY_API_KEY']['valueFrom']['secretKeyRef'],{'name':'web-provider','key':'TAVILY_API_KEY'})
   else:self.assertNotIn('TAVILY_API_KEY',env)
  v['research']['webSearch']['secretName']='';self.render(v,success=False)
  self.assertFalse(self.values()['research']['webSearch']['enabled'])
 def test_workload_routing_is_explicit_and_confined_to_ordinary_research(self):
  v=self.values();v['routing']['enabled']=True;v['routing']['researchEnabled']=True;v['routing']['image']='registry.example/switchyard@sha256:'+'b'*64
  docs=self.render(v);deploy=[d for d in docs if d['kind']=='Deployment'];self.assertEqual(len(deploy),17)
  for d in deploy:
   pod=d['spec']['template']['spec'];c=pod['containers'][0];env={e['name']:e for e in c.get('env',[])}
   if d['metadata']['name']=='rag-switchyard':
    self.assertFalse(pod['automountServiceAccountToken']);self.assertEqual(pod['schedulerName'],'runai-scheduler');self.assertFalse(any('secret' in e for e in c.get('env',[])))
    self.assertFalse(any('persistentVolumeClaim' in vol or 'secret' in vol for vol in pod['volumes']));self.assertNotIn('nvidia.com/gpu',c['resources'].get('limits',{}))
   elif d['metadata']['name']=='rag-research':
    self.assertEqual(env['RESEARCH_ROUTING_ENABLED']['value'],'true');self.assertTrue(any(m['mountPath']=='/routing' and m['readOnly'] for m in c['volumeMounts']))
   else:self.assertNotIn('RESEARCH_ROUTING_ENABLED',env)
  v['routing']['enabled']=False;self.render(v,success=False)
  v=self.values();self.assertFalse(v['routing']['enabled']);self.assertFalse(v['routing']['researchEnabled'])
 def test_search_routing_mount_and_provider_credential_are_separate(self):
  v=self.values();v['routing'].update(enabled=True,searchEnabled=True,image='registry.example/switchyard@sha256:'+'b'*64)
  v['routing']['hosted'].update(enabled=True,secretName='hosted-provider')
  docs=self.render(v)
  for d in docs:
   if d['kind']!='Deployment':continue
   c=d['spec']['template']['spec']['containers'][0];env={e['name']:e for e in c.get('env',[])}
   if d['metadata']['name']=='rag-api':self.assertEqual(env['SEARCH_ROUTING_ENABLED']['value'],'true');self.assertNotIn('NVIDIA_REMOTE_API_KEY',env)
   elif d['metadata']['name']=='rag-switchyard':self.assertEqual(env['NVIDIA_REMOTE_API_KEY']['valueFrom']['secretKeyRef']['name'],'hosted-provider')
   else:self.assertNotIn('SEARCH_ROUTING_ENABLED',env);self.assertNotIn('NVIDIA_REMOTE_API_KEY',env)
  v['routing']['hosted']['secretName']='';self.render(v,success=False)
  v['routing']['enabled']=False;self.render(v,success=False)
 def test_defaults_reject_unconfigured_installation(self):
  self.render(yaml.safe_load((ROOT/'charts/agentic-rag/values.yaml').read_text()),success=False)
 def test_application_roles_secrets_and_readiness(self):
  docs=self.render(self.values());deploy=[d for d in docs if d['kind']=='Deployment'];self.assertEqual(len(deploy),16)
  self.assertEqual(len([d for d in docs if d['kind']=='Job']),1)
  self.assertFalse(any(d['kind']=='Secret' for d in docs))
  for d in deploy:
   s=d['spec']['template']['spec'];self.assertEqual(s['schedulerName'],'runai-scheduler');self.assertFalse(s['automountServiceAccountToken'])
   self.assertEqual(d['spec']['template']['metadata']['labels']['runai/queue'],'validation')
   self.assertEqual(s['initContainers'][0]['name'],'wait-for-schema-and-tokenizers')
   self.assertEqual(next(e['value'] for e in s['containers'][0]['env'] if e['name']=='NVIDIA_VISIBLE_DEVICES'),'void')
   self.assertNotIn('run.ai/control-plane-managed',d['metadata']['labels'])
   self.assertTrue(any(e['name']=='MILVUS_TOKEN' and 'secretKeyRef' in e['valueFrom'] for e in s['containers'][0]['env']))
 def test_private_roles_are_isolated_and_operator_credentials_are_references(self):
  v=self.values();v['privateAnalysis']['enabled']=True;v['secrets']['elasticsearchCA']='';docs=self.render(v)
  deploy=[d for d in docs if d['kind']=='Deployment'];self.assertEqual(len(deploy),24)
  api=next(d for d in deploy if d['metadata']['name']=='rag-api');self.assertEqual(api['spec']['template']['metadata']['labels']['rag-private-client'],'true')
  for d in deploy:
   if not d['metadata']['name'].startswith('rag-analysis-') or d['metadata']['name'].endswith('-redis'):continue
   pod=d['spec']['template']['spec'];self.assertEqual(pod['schedulerName'],'runai-scheduler')
   c=pod['containers'][0];env={e['name']:e for e in c['env']}
   self.assertEqual(env['POSTGRES_DSN']['value'],'postgresql://unavailable/permanent-not-mounted')
   self.assertEqual(env['PRIVATE_POSTGRES_DSN']['valueFrom']['secretKeyRef']['name'],'rag-private-runtime')
   self.assertFalse(any(x['name'] in ('artifacts','approved-source','local-admin') for x in pod['volumes']))
   self.assertFalse(any(x['name']=='es-ca' for x in pod['volumes']))
   self.assertFalse(any(e['name']=='ES_CA' for e in c['env']))
   self.assertNotIn('nvidia.com/gpu',c['resources'].get('limits',{}))
   self.assertEqual(pod['initContainers'][0]['name'],'wait-for-private-schema')
  self.assertFalse(any(d['kind']=='Secret' for d in docs))
  self.assertTrue(any(d['kind']=='Job' and '-private-schema-' in d['metadata']['name'] for d in docs))
 def test_http_elasticsearch_never_loads_a_missing_ca(self):
  v=self.values();v['secrets']['elasticsearchCA']='';docs=self.render(v)
  for d in docs:
   if d['kind']!='Deployment':continue
   s=d['spec']['template']['spec'];self.assertFalse(any(x['name']=='es-ca' for x in s['volumes']))
   self.assertFalse(any(e['name']=='ES_CA' for e in s['containers'][0]['env']))
 def test_embedding_identity_matches_application_contract(self):
  docs=self.render(self.values());foreground=next(d for d in docs if d['kind']=='Deployment' and d['metadata']['name']=='rag-foreground')
  env={e['name']:e.get('value') for e in foreground['spec']['template']['spec']['containers'][0]['env']}
  config={'model':'nvidia/Nemotron-3.5-Embed-1B-NVFP4','checkpoint':'a'*64,'dimensions':2048,'modality':'text','input_type':'passage','truncate':'NONE','library':'26.8.2'}
  self.assertEqual(env['EMBED_CONFIG_DIGEST'],'sha256:'+hashlib.sha256(json.dumps(config,sort_keys=True,separators=(',',':')).encode()).hexdigest())
  self.assertEqual(env['EMBED_MODEL_DIGEST'],'sha256:'+'a'*64)
 def test_source_root_changes_roll_configuration(self):
  v=self.values();a=self.render(v);v['configuration']['allowedSourceRoots']={'approved':'/source-roots/approved'};b=self.render(v)
  def checksum(docs):return next(d for d in docs if d['kind']=='Deployment')['spec']['template']['metadata']['annotations']['checksum/config']
  self.assertNotEqual(checksum(a),checksum(b))
 def test_storage_and_tokenizer_preparation_are_explicit(self):
  v=self.values();v['configuration']['prepareTokenizers']=True
  for s in v['storage'].values():s['create']=True
  docs=self.render(v);pvcs=[d for d in docs if d['kind']=='PersistentVolumeClaim'];self.assertEqual(len(pvcs),4)
  self.assertTrue(all(d['metadata']['annotations']['helm.sh/resource-policy']=='keep' for d in pvcs))
  self.assertTrue(any(d['kind']=='Job' and '-tokenizers-' in d['metadata']['name'] for d in docs))
 def enabled_models(self):
  v=self.values()
  for name,m in v['models'].items():
   m.update(enabled=True,checkpointClaim='checkpoint',cacheClaim='cache',cacheSubPath='sku/sm/driver/model/revision',profile='a'*64,gemmBackend='cutlass-nvfp4-sm120',modelSnapshot='hf-validation',checkpointSubPath='validated/lightning')
  return v
 def test_all_models_use_runai_and_separate_checkpoint_cache(self):
  docs=self.render(self.enabled_models());models=[d for d in docs if d['kind']=='Deployment' and '-model-' in d['metadata']['name']];self.assertEqual(len(models),10)
  for d in models:
   pod=d['spec']['template'];c=pod['spec']['containers'][0];self.assertTrue(c['image'].endswith(':latest'));self.assertEqual(c['imagePullPolicy'],'Always')
   self.assertEqual(pod['spec']['schedulerName'],'runai-scheduler');self.assertEqual(c['readinessProbe']['httpGet']['path'],'/v1/health/ready')
   mounts=c['volumeMounts'];self.assertTrue(any(m['name']=='checkpoint' for m in mounts));self.assertTrue(any(m['name']=='runtime-cache' and 'subPath' in m for m in mounts))
   if d['metadata']['name'].endswith('-llm'):self.assertEqual(c['resources']['limits']['nvidia.com/gpu'],1)
   else:self.assertEqual(pod['metadata']['annotations']['gpu-fraction'],'0.4' if d['metadata']['name'].endswith('-lightning') else '0.25');self.assertNotIn('nvidia.com/gpu',c['resources'].get('limits',{}))
 def test_lightning_has_readonly_checkpoint_bytes_and_explicit_capacity_example(self):
  v=self.enabled_models();v['models']['lightning']['gpuFraction']=.4
  docs=self.render(v);d=next(x for x in docs if x['kind']=='Deployment' and x['metadata']['name']=='rag-model-lightning');pod=d['spec']['template'];c=pod['spec']['containers'][0]
  self.assertEqual(pod['metadata']['annotations']['gpu-fraction'],'0.4');self.assertFalse(pod['spec']['automountServiceAccountToken']);self.assertIn('262144',c['args'])
  protected=[m for m in c['volumeMounts'] if m['name']=='checkpoint'];self.assertTrue(all(m['readOnly'] for m in protected));self.assertTrue(any('/blobs' in m['mountPath'] for m in protected));self.assertTrue(any('snapshots/hf-validation' in m['mountPath'] for m in protected))
  self.assertIn('prepare-runtime-cache',[x['name'] for x in pod['spec']['initContainers']])
 def test_invalid_fraction_is_rejected(self):
  for value in (0,-0.5,1.1):
   v=self.enabled_models();v['models']['embed']['gpuFraction']=value;self.render(v,success=False)
 def test_model_image_override_keeps_latest_policy(self):
  v=self.enabled_models();v['models']['embed']['image']='registry.example/nim:old';self.render(v,success=False)
 def test_datastore_persistence_and_milvus_gpu(self):
  v=yaml.safe_load((ROOT/'charts/agentic-rag-infrastructure/values.yaml').read_text());v['runai']['projectName']='validation';docs=self.render(v,'agentic-rag-infrastructure')
  self.assertEqual(len([d for d in docs if d['kind']=='StatefulSet']),6)
  self.assertTrue(all(d['metadata']['annotations']['helm.sh/resource-policy']=='keep' for d in docs if d['kind']=='PersistentVolumeClaim'))
  for d in docs:
   if d['kind']!='StatefulSet':continue
   s=d['spec']['template']['spec'];self.assertEqual(s['schedulerName'],'runai-scheduler');self.assertFalse(s['automountServiceAccountToken'])
   if d['metadata']['name'].endswith('milvus'):self.assertEqual(s['containers'][0]['resources']['limits']['nvidia.com/gpu'],1)
   else:self.assertNotIn('nvidia.com/gpu',s['containers'][0]['resources'].get('limits',{}))
 def test_milvus_local_dependencies_cannot_be_omitted(self):
  v=yaml.safe_load((ROOT/'charts/agentic-rag-infrastructure/values.yaml').read_text());v['runai']['projectName']='validation';v['components']['etcd']['enabled']=False;self.render(v,'agentic-rag-infrastructure',success=False)


@unittest.skipUnless((ROOT/'charts/agentic-rag/files/wait_ready.py').is_file(),'Requires source chart readiness helper')
class StartupGateBehavior(unittest.IsolatedAsyncioTestCase):
 async def test_valid_older_tokenizer_contract_does_not_release_workers(self):
  import functools,os,runpy,sys,types
  from unittest.mock import AsyncMock,patch
  for stale in ('checkpoint','limit'):
   with self.subTest(stale=stale),tempfile.TemporaryDirectory() as tmp:
    folder=Path(tmp);files={'tokenizer.json':'a'*64}
    (folder/'legal.json').write_text(json.dumps({'files':files}))
    (folder/'embed.json').write_text(json.dumps(files))
    state={'ready':False};connections=[]
    class Connection:
     async def fetch(self,*args):return [{'name':'migration','sha256':'expected'}]
     async def close(self):self.closed=True
    async def connect(*args,**kwargs):
     c=Connection();c.closed=False;connections.append(c);return c
    @functools.lru_cache()
    def budget(identity=None):
     return types.SimpleNamespace(checkpoint=('b'*64 if stale=='checkpoint' and not state['ready'] else 'a'*64),max_characters=(10 if stale=='limit' and not state['ready'] else 100),identity='identity')
    async def advance(*args):state['ready']=True
    modules={'asyncpg':types.SimpleNamespace(connect=connect),'application.migrate':types.SimpleNamespace(migrations=lambda root:[('migration','expected','')]),'application.legal_text_budget':types.SimpleNamespace(load_budget=budget)}
    env={'POSTGRES_DSN':'not-a-network-dsn','EXPECTED_EMBED_CHECKPOINT_SHA256':'a'*64,'EXPECTED_EMBED_MAX_CHARACTERS':'100','LEGAL_INPUT_CONTRACT':str(folder/'legal.json'),'EMBED_INPUT_CONTRACT':str(folder/'embed.json')}
    with patch.dict(sys.modules,modules),patch.dict(os.environ,env),patch('asyncio.sleep',AsyncMock(side_effect=advance)) as sleep:
     namespace=runpy.run_path(str(ROOT/'charts/agentic-rag/files/wait_ready.py'),run_name='gate_test')
     await namespace['main']()
     self.assertTrue(state['ready']);self.assertEqual(sleep.await_count,1);self.assertEqual(len(connections),2);self.assertTrue(all(c.closed for c in connections))


class LocalCredentialBootstrap(unittest.TestCase):
 def test_existing_secrets_are_never_rotated_implicitly(self):
  import runpy,sys
  from unittest.mock import patch
  module=runpy.run_path(str(ROOT/'tools/local_secrets.py'),run_name='bootstrap_test')
  with patch.object(sys,'argv',['local_secrets','--namespace','validation']),patch('subprocess.check_output',return_value='secret/existing'),patch('subprocess.run') as create:
   with self.assertRaisesRegex(SystemExit,'refusing automatic rotation'):module['main']()
   create.assert_not_called()
 def test_generated_references_match_datastore_credentials(self):
  import base64,runpy
  module=runpy.run_path(str(ROOT/'tools/local_secrets.py'),run_name='bootstrap_test')
  data,runtime=module['resources']('validation','rag-data','rag-runtime','rag-datastores')
  decode=lambda d:{k:base64.b64decode(v).decode() for k,v in d['data'].items()}
  passwords,client=decode(data),decode(runtime)
  self.assertIn(passwords['POSTGRES_PASSWORD'],client['POSTGRES_DSN'])
  self.assertIn(passwords['REDIS_PASSWORD'],client['REDIS_URL'])
  self.assertEqual(client['ES_WRITER_PASSWORD'],passwords['ELASTIC_PASSWORD'])
  self.assertEqual(client['ES_READER_PASSWORD'],passwords['ELASTIC_PASSWORD'])

if __name__=='__main__':unittest.main()
