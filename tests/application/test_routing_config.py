import importlib.util,json,tempfile,tomllib,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]

class RoutingConfigTests(unittest.TestCase):
 def render(self,**kwargs):
  spec=importlib.util.spec_from_file_location('routing_render',ROOT/'deployment/routing/render.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  return m.render(efficient_endpoint='http://efficient-model:8000/v1',capable_endpoint='http://capable-model:8000/v1',efficient_model='model/efficient',capable_model='model/capable',router_endpoint='http://rag-agentic-switchyard:4000/v1',**kwargs)
 def test_research_classifies_once_per_phase_and_coding_is_composite(self):
  files=self.render();d=tomllib.loads(files['routes.toml']);client=json.loads(files['research-client.json'])
  self.assertEqual(client['targets'],{'model/efficient':'research-efficient','model/capable':'research-capable'})
  for name in client['phases'].values():
   r=next(r for r in d['routes'].values() if r['id']==name);self.assertEqual(r['type'],'llm_classifier');self.assertEqual(r['mode'],'custom');self.assertEqual(r['classify_trigger'],'new_session')
  self.assertEqual(d['routes']['coding']['type'],'composite');self.assertEqual(d['routes']['coding']['classifier']['classify_trigger'],'new_session')
  self.assertEqual(d['routes']['coding']['stage']['capable_target'],'capable');self.assertEqual(d['routes']['coding']['stage']['efficient_target'],'efficient')
  self.assertNotIn('api_key_env',files['routes.toml']);self.assertNotIn('transcript_max_chars',files['routes.toml']);self.assertNotIn('prefill',files['routes.toml'])
 def test_policy_identity_changes_with_model_configuration(self):
  first=self.render();second=self.render(context_window=32768)
  self.assertNotEqual(json.loads(first['research-client.json'])['policy_sha256'],json.loads(second['research-client.json'])['policy_sha256'])
 def test_rejects_same_models_bad_urls_and_silent_hosted_endpoints(self):
  spec=importlib.util.spec_from_file_location('routing_render',ROOT/'deployment/routing/render.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  base=dict(efficient_endpoint='http://efficient:8000/v1',capable_endpoint='http://capable:8000/v1',efficient_model='model/efficient',capable_model='model/capable',router_endpoint='http://router:4000/v1')
  for changes in [{'capable_model':'model/efficient'},{'efficient_endpoint':'https://api.openai.com/v1'},{'efficient_endpoint':'http://user:pwd@efficient/v1'},{'router_endpoint':'http://router/v1?key=secret'}]:
   with self.subTest(changes=changes),self.assertRaises(ValueError):m.render(**{**base,**changes})
 def test_native_router_is_cpu_only_and_has_no_application_secrets_or_storage(self):
  spec=importlib.util.spec_from_file_location('routing_native',ROOT/'deployment/routing/native.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  base={'name':'rag-agentic-api','projectId':'example-project','clusterId':'example-cluster','spec':{'image':'old','security':{'hostNetwork':False,'allowPrivilegeEscalation':False},'compute':{},'storage':{'pvc':[{'name':'must-not-copy'}]},'environmentVariables':[{'name':'POSTGRES_DSN','secret':{'name':'must-not-copy','key':'DSN'}}],'nodeAffinityRequired':{'nodeSelectorTerms':[]}}}
  result=m.render_native(base,self.render(),image='registry.internal/switchyard@sha256:'+'a'*64,namespace='example-ns')
  workload=result['workspace'];self.assertEqual(workload['projectId'],'example-project');self.assertEqual(workload['name'],'rag-agentic-switchyard')
  body=json.dumps(workload);self.assertNotIn('POSTGRES_DSN',body);self.assertNotIn('must-not-copy',body);self.assertNotIn('gpu',json.dumps(workload['spec']['compute']).lower());self.assertFalse(workload['spec']['security']['hostNetwork'])
  self.assertEqual(result['configmap']['kind'],'ConfigMap');self.assertIn('routes.toml',result['configmap']['data']);self.assertRegex(result['configmap']['metadata']['name'],r'rag-agentic-routing-[a-f0-9]{12}$');self.assertEqual(result['service']['spec']['selector'],{'workloadName':'rag-agentic-switchyard'})

class NativeYamlRouterTests(unittest.TestCase):
 def test_exact_cpu_security_and_secret_reference_are_preserved(self):
  spec=importlib.util.spec_from_file_location('routing_native_v2',ROOT/'deployment/routing/native.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  base={'name':'rag-agentic-switchyard','projectId':'example-project','spec':{'image':'registry.example/router@sha256:'+'a'*64,'environmentVariables':[{'name':'NVIDIA_VISIBLE_DEVICES','value':'void'},{'name':'NVIDIA_REMOTE_API_KEY','secret':{'name':'provider','key':'NVIDIA_REMOTE_API_KEY'}}]}}
  prepared={'workspace':base,'configmap':{'metadata':{'name':'routing-policy'}}}
  result=m.render_v2(prepared,namespace='example-ns',project_name='example',node_selector={'kubernetes.io/hostname':'example-node'})
  pod=result['manifest']['spec']['template']['spec'];self.assertFalse(pod['automountServiceAccountToken']);self.assertNotIn('clusterId',result['metadata']);self.assertEqual(result['metadata']['projectId'],'example-project')
  c=pod['containers'][0];self.assertEqual(c['env'][1]['valueFrom']['secretKeyRef']['name'],'provider');self.assertFalse(any('gpu' in k for k in c['resources']['limits']));self.assertFalse(any('persistentVolumeClaim' in v for v in pod['volumes']))
