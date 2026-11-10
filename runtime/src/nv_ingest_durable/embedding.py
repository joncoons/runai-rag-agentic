"""Model-pinned remote text embedding adapter for model-pinned remote text embedding actors.

No model registration aliases, online tokenizer downloads or global monkey patches.
Requires the source-locked dimension patches and an exact local tokenizer manifest.
"""
from pathlib import Path
import hashlib
import importlib
import json
import math
from .admission import require_successful_upstream

class ExactTokenizer:
    def __init__(self, tokenizer):
        # Saved inference defaults can truncate/pad before admission sees the length.
        # Count the complete input without changing the checkpoint bytes or vocabulary.
        tokenizer.no_truncation()
        tokenizer.no_padding()
        self.tokenizer=tokenizer
    def encode(self,text,*,add_special_tokens=False):return self.tokenizer.encode(text,add_special_tokens=add_special_tokens).ids
    def decode(self,ids,*,skip_special_tokens=True):return self.tokenizer.decode(ids,skip_special_tokens=skip_special_tokens)
    def encode_batch(self,texts,*,add_special_tokens=False):return [x.ids for x in self.tokenizer.encode_batch(texts,add_special_tokens=add_special_tokens)]

class RemoteEmbedding:
    def __init__(self, params, *, checkpoint, expected_files):
        if params.dimensions!=2048:raise ValueError('Ingestion requires 2048 dimensions')
        if params.embed_modality!='text':raise ValueError('This adapter admits text embeddings only')
        root=Path(checkpoint)
        required={'tokenizer.json','config_sentence_transformers.json','sentence_bert_config.json'}
        if set(expected_files)!=required:raise ValueError('Exact tokenizer/prompt/input-limit manifest required')
        for name,digest in expected_files.items():
            if hashlib.sha256((root/name).read_bytes()).hexdigest()!=digest:raise ValueError('Checkpoint input-contract hash mismatch: '+name)
        from tokenizers import Tokenizer
        self.tokenizer=ExactTokenizer(Tokenizer.from_file(str(root/'tokenizer.json')))
        self.max_tokens=int(json.loads((root/'sentence_bert_config.json').read_text())['max_seq_length'])
        prompts=json.loads((root/'config_sentence_transformers.json').read_text())['prompts']
        self.prefix=prompts['query' if params.input_type=='query' else 'document']
        self.params=params
        module=importlib.import_module('nemo_retriever.operators.embed.cpu_operator')
        self.actor=module._BatchEmbedCPUActor(params)
        schema=importlib.import_module('nemo_retriever.common.schemas.embedding')
        self.select_text=lambda row:schema.embedding_text_input(row,text_column=params.text_column)
        policy=importlib.import_module('nemo_retriever.models.inference.embedding_input').EmbeddingInputPolicy
        self.actor._kwargs['embedding_input_policy']=policy(tokenizer=self.tokenizer,max_tokens=self.max_tokens,prefix=self.prefix,prefix_if_missing=False)
    def process(self, frame):
        require_successful_upstream(frame)
        if frame.empty:return frame.copy()
        attributes=dict(frame.attrs)
        for _,row in frame.iterrows():
            if row.get('_embed_modality','text')!='text':raise ValueError('Only text modality qualified')
            text=self.select_text(row)
            if not isinstance(text,str) or not text.strip():raise ValueError('Empty searchable embedding input')
            count=len(self.tokenizer.encode(self.prefix+text.strip(),add_special_tokens=True))
            if count>self.max_tokens:raise ValueError('Embedding input exceeds checkpoint token limit; splitting/truncation requires explicit policy')
        result=self.actor.process(frame)
        if len(result)!=len(frame) or not result.index.equals(frame.index):raise ValueError('Embedding changed input cardinality or row identity')
        for _,row in result.iterrows():
            payload=row.get(self.params.output_column)
            vector=payload.get('embedding') if isinstance(payload,dict) else None
            if vector is None:
                metadata=row.get('metadata');vector=metadata.get('embedding') if isinstance(metadata,dict) else None
            if not isinstance(vector,list) or len(vector)!=2048 or any(isinstance(x,bool) or not isinstance(x,(int,float)) or not math.isfinite(x) for x in vector):
                raise ValueError('Embedding output violates finite 2048-dimensional complete-coverage contract')
        result.attrs.update(attributes)
        return result
