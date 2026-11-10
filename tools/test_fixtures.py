"""Generated tokenizer/model contracts for offline tests; never deployment defaults."""
import hashlib,json,os,re
from pathlib import Path
from tokenizers import Tokenizer,models,pre_tokenizers

def prepare(root,output):
 output=Path(output);output.mkdir(parents=True,exist_ok=True)
 tokenizer=Tokenizer(models.WordLevel({"[UNK]":0},unk_token="[UNK]"));tokenizer.pre_tokenizer=pre_tokenizers.Whitespace()
 tokenizer.save(str(output/"tokenizer.json"))
 (output/"config_sentence_transformers.json").write_text(json.dumps({"prompts":{"document":"passage: ","query":"query: "}}))
 (output/"sentence_bert_config.json").write_text(json.dumps({"max_seq_length":32768}))
 files={n:hashlib.sha256((output/n).read_bytes()).hexdigest() for n in ("tokenizer.json","config_sentence_transformers.json","sentence_bert_config.json")}
 (output/"embedding-input-contract.json").write_text(json.dumps(files))
 legal={"files":files,"tokenizer_directory":".","checkpoint_sha256":"a"*64,"max_request_characters":65536}
 (output/"legal-input-contract.json").write_text(json.dumps(legal))
 identity=hashlib.sha256(json.dumps(legal,sort_keys=True,separators=(",",":")).encode()).hexdigest()
 (output/"legal-input-contracts").mkdir()
 (output/"legal-input-contracts"/(identity+".json")).write_text(json.dumps(legal))
 template=(root/"config/model-contracts.json.template").read_text()
 model=re.sub(r"\$\{[A-Z0-9_]+\}","a"*64,template)
 obj=json.loads(model)
 for entry in obj["extraction_model_contract"].values():
  entry["platform_image_digest"]="sha256:"+"a"*64
 (output/"model-contracts.json").write_text(json.dumps(obj))
 os.environ.update(LEGAL_INPUT_CONTRACT=str(output/"legal-input-contract.json"),EMBED_INPUT_CONTRACT=str(output/"embedding-input-contract.json"),MODEL_CONTRACTS_FILE=str(output/"model-contracts.json"),TEST_EMBED_CHECKPOINT=str(output))
