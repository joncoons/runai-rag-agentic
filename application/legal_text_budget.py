"""Exact, immutable embedding input contract for legal source grouping."""
import hashlib,json,os
from functools import lru_cache
from pathlib import Path

class LegalTextBudget:
    def __init__(self,root,manifest):
        root=Path(root)
        for name,digest in manifest['files'].items():
            if hashlib.sha256((root/name).read_bytes()).hexdigest()!=digest:raise ValueError('Legal tokenizer input contract mismatch: '+name)
        from tokenizers import Tokenizer
        self.tokenizer=Tokenizer.from_file(str(root/'tokenizer.json'));self.tokenizer.no_truncation();self.tokenizer.no_padding()
        self.prefix=json.loads((root/'config_sentence_transformers.json').read_text())['prompts']['document']
        self.max_tokens=int(json.loads((root/'sentence_bert_config.json').read_text())['max_seq_length'])
        self.checkpoint=manifest['checkpoint_sha256']
        self.max_characters=manifest['max_request_characters']
        self.identity=hashlib.sha256(json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    @lru_cache(maxsize=128)
    def fits(self,text):
        return len(text)<=self.max_characters and len(self.tokenizer.encode(self.prefix+text,add_special_tokens=True).ids)<=self.max_tokens

@lru_cache(maxsize=8)
def load_budget(identity=None):
    current=Path(os.environ.get('LEGAL_INPUT_CONTRACT','/app/config/legal-input-contract.json'))
    if identity is not None and (len(identity)!=64 or any(c not in '0123456789abcdef' for c in identity)):raise ValueError('Invalid legal input contract identity')
    path=current.parent/'legal-input-contracts'/(identity+'.json') if identity else current
    manifest=json.loads(path.read_text())
    budget=LegalTextBudget(current.parent/manifest['tokenizer_directory'],manifest)
    if identity is not None and budget.identity!=identity:raise ValueError('Historical legal input contract identity differs')
    return budget
