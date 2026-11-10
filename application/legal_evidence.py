"""Grounded legal evidence roles; classifications do not mutate admitted bytes."""
import re
from functools import lru_cache
from urllib.parse import unquote,urlsplit
from collection_control.artifacts import load_artifact,record_digest


def legal_role(record):
    metadata=record.get('metadata',{})
    if metadata.get('content_role')=='legal_disclosure':return 'legal_disclosure'
    path=unquote(urlsplit(metadata.get('provenance',{}).get('canonical_uri','')).path).lower()
    filename=path.rsplit('/',1)[-1]
    if re.search(r'\.(pdf|docx?|txt|md)$',filename) and re.search(r'(?:agreement|disclosure|terms[-_ ](?:and[-_ ])?conditions|certificate[-_ ]of[-_ ]insurance)',filename):
        return 'legal_document'
    return 'primary'


@lru_cache(maxsize=4)
def snapshot_legal_hashes(root,sha):
    # load_artifact validates the immutable artifact identity. Only role hashes,
    # not full vectors or document bodies, are retained in this bounded cache.
    artifact=load_artifact(root,sha)
    return frozenset(record_digest(r) for r in artifact['records'] if legal_role(r)!='primary')
