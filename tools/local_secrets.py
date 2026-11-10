"""Create fresh local-stack credential Secrets without putting passwords in values or argv.

Run only against the intended registered project namespace. Existing credentials
are never overwritten; rotate them explicitly instead of re-running this helper.
"""
import argparse,base64,json,secrets,subprocess
from urllib.parse import quote

def resources(namespace,data_release,runtime_name,datastore_name):
    passwords={name:secrets.token_hex(24) for name in ('POSTGRES_PASSWORD','REDIS_PASSWORD','ELASTIC_PASSWORD','MINIO_ROOT_PASSWORD')}
    passwords['MINIO_ROOT_USER']='rag-objects'
    runtime={
        'POSTGRES_DSN':f"postgresql://rag:{quote(passwords['POSTGRES_PASSWORD'],safe='')}@{data_release}-postgres:5432/rag",
        'REDIS_URL':f"redis://:{quote(passwords['REDIS_PASSWORD'],safe='')}@{data_release}-redis:6379/0",
        'ES_ENDPOINT':f'http://{data_release}-elasticsearch:9200',
        'ES_READER_USER':'elastic','ES_READER_PASSWORD':passwords['ELASTIC_PASSWORD'],
        'ES_WRITER_USER':'elastic','ES_WRITER_PASSWORD':passwords['ELASTIC_PASSWORD'],
        'MILVUS_URI':f'http://{data_release}-milvus:19530','MILVUS_TOKEN':''}
    def secret(name,values):
        return {'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':namespace},'type':'Opaque','data':{k:base64.b64encode(v.encode()).decode() for k,v in values.items()}}
    return [secret(datastore_name,passwords),secret(runtime_name,runtime)]

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--namespace',required=True);p.add_argument('--data-release',default='rag-data');p.add_argument('--runtime-secret',default='rag-runtime');p.add_argument('--datastore-secret',default='rag-datastores');p.add_argument('--dry-run',action='store_true');a=p.parse_args()
    if a.dry_run:print('Would create Secrets:',a.datastore_secret,a.runtime_secret,'in',a.namespace);return
    existing=subprocess.check_output(['kubectl','-n',a.namespace,'get','secret',a.datastore_secret,a.runtime_secret,'--ignore-not-found','-o','name'],text=True)
    if existing.strip():raise SystemExit('Credential Secret already exists; refusing automatic rotation or partial replacement')
    for resource in resources(a.namespace,a.data_release,a.runtime_secret,a.datastore_secret):
        subprocess.run(['kubectl','create','-f','-'],input=json.dumps(resource),text=True,check=True)
    print('Created local-stack credential Secrets; values were not printed')

if __name__=='__main__':main()
