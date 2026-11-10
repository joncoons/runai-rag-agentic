from dataclasses import dataclass
import json
import os
from pathlib import Path

@dataclass(frozen=True)
class Settings:
    dsn:str
    redis_url:str
    root:Path
    contract:dict
    models:dict
    es_endpoint:str
    es_user:str
    es_password:str
    es_ca:str|None
    milvus_uri:str
    milvus_token:str|None
    llm_endpoint:str
    llm_model:str

    @classmethod
    def load(cls):
        models=json.loads(Path(os.environ.get('MODEL_BINDINGS_FILE','/app/config/model-bindings.json')).read_text())
        contract={'dimensions':2048,'dtype':'float32','normalization':'unit','metric':'IP','schema_version':1,'embedding_checkpoint_sha256':models['embedding_contract']['checkpoint_content_manifest_sha256']}
        return cls(os.environ['POSTGRES_DSN'],os.environ['REDIS_URL'],Path(os.environ['ARTIFACT_ROOT']),contract,models,os.environ['ES_ENDPOINT'],os.environ['ES_USER'],os.environ['ES_PASSWORD'],os.environ.get('ES_CA'),os.environ['MILVUS_URI'],os.environ.get('MILVUS_TOKEN'),os.environ['LLM_ENDPOINT'].rstrip('/'),os.environ['LLM_MODEL'])

    @property
    def snapshots(self):return self.root/'snapshots'
    @property
    def raw(self):return self.root/'raw'
    @property
    def stages(self):return self.root/'stages'
