#!/usr/bin/env python3
"""Prepare native Run:ai CPU workspace/service artifacts without submitting them."""
import argparse,copy,json,re
from pathlib import Path

def render_native(base,files,*,image,namespace,hosted_secret=None):
 if not re.fullmatch(r'[^\s]+@sha256:[a-f0-9]{64}',image):raise ValueError('Supply the qualified immutable Switchyard image')
 name='rag-agentic-switchyard';policy=json.loads(files['research-client.json'])['policy_sha256'];config='rag-agentic-routing-'+policy[:12];row={k:copy.deepcopy(base[k]) for k in ('projectId','clusterId')};row['name']=name
 row['spec']={'image':image,'imagePullPolicy':'IfNotPresent','command':'switchyard-server','args':'--config /routing/routes.toml --host 0.0.0.0 --port 4000','createHomeDir':False,
  'compute':{'cpuCoreRequest':.25,'cpuCoreLimit':2,'cpuMemoryRequest':'256Mi','cpuMemoryLimit':'1Gi'},
  'security':{'hostNetwork':False,'hostIpc':False,'allowPrivilegeEscalation':False,'runAsNonRoot':True,'uidGidSource':'custom','runAsUid':1000,'runAsGid':1000,'readOnlyRootFilesystem':True},
  'storage':{'configMapVolume':[{'name':'routing','configMap':config,'mountPath':'/routing'}]},
  'environmentVariables':[{'name':'NVIDIA_VISIBLE_DEVICES','value':'void'},{'name':'CUDA_VISIBLE_DEVICES','value':''}],
  'probes':{'readiness':{'initialDelaySeconds':2,'periodSeconds':5,'timeoutSeconds':3,'failureThreshold':3,'handler':{'httpGet':{'path':'/v1/models','port':4000,'scheme':'HTTP'}}}},
  'labels':[{'name':'rag-agentic-component','value':'application'},{'name':'rag-agentic-role','value':'switchyard'}],'restartPolicy':'Always','preemptibility':'non-preemptible'}
 if hosted_secret:
  if not re.fullmatch('[a-z0-9]([-a-z0-9]*[a-z0-9])?',hosted_secret):raise ValueError('Invalid namespace-local hosted credential secret name')
  row['spec']['environmentVariables'].append({'name':'NVIDIA_REMOTE_API_KEY','secret':{'name':hosted_secret,'key':'NVIDIA_REMOTE_API_KEY'}})
 if 'nodeAffinityRequired' in base['spec']:row['spec']['nodeAffinityRequired']=copy.deepcopy(base['spec']['nodeAffinityRequired'])
 configmap={'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':config,'namespace':namespace},'data':{k:files[k] for k in ('routes.toml','research-client.json','search-client.json') if k in files}}
 service={'apiVersion':'v1','kind':'Service','metadata':{'name':name,'namespace':namespace},'spec':{'selector':{'workloadName':name},'ports':[{'name':'http','port':4000,'targetPort':4000,'protocol':'TCP'}]}}
 return {'status':'prepared-not-deployed','native_admission_requirements':['Disable automatic service-account token mounting for this CPU-only workspace; verify the admitted pod before activation.'],'workspace':row,'configmap':configmap,'service':service,
  'search_environment':[{'name':'SEARCH_ROUTING_ENABLED','value':'false'},{'name':'SEARCH_ROUTING_CONFIG_FILE','value':'/routing/search-client.json'}],
  'research_environment':[{'name':'RESEARCH_ROUTING_ENABLED','value':'false'},{'name':'RESEARCH_ROUTING_CONFIG_FILE','value':'/routing/research-client.json'}],
  'research_storage':{'configMapVolume':[{'name':'routing-client','configMap':config,'mountPath':'/routing'}]}}

def render_v2(prepared,*,namespace,project_name,node_selector=None):
 """Native Via YAML variant preserving exact CPU pod security."""
 w=prepared['workspace'];name=w['name']
 if name!='rag-agentic-switchyard' or not re.fullmatch(r'[a-z0-9]([-a-z0-9]*[a-z0-9])?',project_name):raise ValueError('Invalid router/project identity')
 labels={'app':name,'workloadName':name,'app.kubernetes.io/part-of':'rag-agentic','runai/queue':project_name}
 env=[{'name':e['name'],'valueFrom':{'secretKeyRef':e['secret']}} if 'secret' in e else copy.deepcopy(e) for e in w['spec']['environmentVariables']]
 pod={'schedulerName':'runai-scheduler','automountServiceAccountToken':False,'nodeSelector':node_selector or {},'securityContext':{'runAsUser':1000,'runAsGroup':1000,'runAsNonRoot':True},
  'containers':[{'name':'switchyard','image':w['spec']['image'],'imagePullPolicy':'IfNotPresent','command':['switchyard-server'],'args':['--config','/routing/routes.toml','--host','0.0.0.0','--port','4000'],'env':env,'ports':[{'containerPort':4000,'name':'http'}],
   'resources':{'requests':{'cpu':'250m','memory':'256Mi'},'limits':{'cpu':'2','memory':'1Gi'}},'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},
   'volumeMounts':[{'name':'routing','mountPath':'/routing','readOnly':True}],'readinessProbe':{'httpGet':{'path':'/v1/models','port':4000},'initialDelaySeconds':2,'periodSeconds':5}}],
  'volumes':[{'name':'routing','configMap':{'name':prepared['configmap']['metadata']['name']}}]}
 manifest={'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':name,'namespace':namespace,'labels':labels},'spec':{'replicas':1,'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':labels},'spec':pod}}}
 return {'metadata':{'name':name,'projectId':w['projectId'],'priority':'very-high','category':'Deploy','preemptibility':'non-preemptible'},'manifest':manifest}

if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--base-request',type=Path,required=True);p.add_argument('--config-directory',type=Path,required=True);p.add_argument('--image',required=True);p.add_argument('--namespace',required=True);p.add_argument('--hosted-secret');p.add_argument('--via-yaml',action='store_true');p.add_argument('--project-name');p.add_argument('--node-selector',default='{}');p.add_argument('--output',type=Path,required=True);args=p.parse_args()
 base=json.loads(args.base_request.read_text());base=base['roles']['api'] if 'roles' in base else base
 files={name:(args.config_directory/name).read_text() for name in ('routes.toml','research-client.json','search-client.json') if (args.config_directory/name).exists()}
 result=render_native(base,files,image=args.image,namespace=args.namespace,hosted_secret=args.hosted_secret);
 if args.via_yaml:
  if not args.project_name:p.error('--via-yaml requires --project-name')
  result={'status':'prepared-not-deployed',**result,'native_v2':render_v2(result,namespace=args.namespace,project_name=args.project_name,node_selector=json.loads(args.node_selector))}
 args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'status':result['status'],'output':str(args.output)}))
