"""Prepare hashed tokenizer contracts without redistributing tokenizer/checkpoint files."""
import argparse,hashlib,json,shutil
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument("--checkpoint",type=Path,required=True);p.add_argument("--checkpoint-sha256",required=True);p.add_argument("--max-request-characters",type=int,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args()
if len(a.checkpoint_sha256)!=64 or any(c not in "0123456789abcdef" for c in a.checkpoint_sha256):raise SystemExit("Use actual checkpoint manifest SHA256")
if a.max_request_characters<=0:raise SystemExit("Use measured serving-model request limit")
files={name:hashlib.sha256((a.checkpoint/name).read_bytes()).hexdigest() for name in ("tokenizer.json","config_sentence_transformers.json","sentence_bert_config.json")}
a.output.mkdir(parents=True,exist_ok=True)
(a.output/"embedding-input-contract.json").write_text(json.dumps(files,indent=2)+"\n")
folder="legal-tokenizers/"+a.checkpoint_sha256;target=a.output/folder;target.mkdir(parents=True,exist_ok=True)
for name in files:shutil.copyfile(a.checkpoint/name,target/name)
contract={"schema_version":"legal-input-contract.v1","checkpoint_sha256":a.checkpoint_sha256,"tokenizer_directory":folder,"max_request_characters":a.max_request_characters,"request_limit_basis":"Operator-verified serving-model input contract; no application payload cap","files":files}
text=json.dumps(contract,sort_keys=True,separators=(",",":"));identity=hashlib.sha256(text.encode()).hexdigest()
(a.output/"legal-input-contract.json").write_text(text+"\n")
(a.output/"legal-input-contracts").mkdir(exist_ok=True);(a.output/"legal-input-contracts"/(identity+".json")).write_text(text+"\n")
print("Prepared tokenizer contract",identity)
