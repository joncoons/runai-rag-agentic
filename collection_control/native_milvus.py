"""Native fields matching the original crawler's text/vector/metadata records."""
import re
from .artifacts import record_digest

def native_handle(handle):
    return isinstance(handle,str) and re.fullmatch(r'rag_agentic_native_[0-9a-f]{32}_mv',handle) is not None

def schema_fields():
    from pymilvus import FieldSchema,DataType
    return [FieldSchema('version_id',DataType.VARCHAR,is_primary=True,max_length=64),
            FieldSchema('record_id',DataType.VARCHAR,max_length=1024),
            FieldSchema('vector',DataType.FLOAT_VECTOR,dim=2048),
            FieldSchema('text',DataType.TEXT),FieldSchema('metadata',DataType.JSON)]

def columns(records):
    return [[record_digest(r) for r in records],[r['record_id'] for r in records],
            [r['vector'] for r in records],[r['text'] for r in records],
            [r['metadata'] for r in records]]

def restore(row):
    return {key:row[key] for key in ('record_id','text','vector','metadata')}
