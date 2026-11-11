"""Grounded legal evidence roles; classifications do not mutate admitted bytes."""
import re,codecs,hashlib,json,os,stat
from pathlib import Path
from functools import lru_cache
from urllib.parse import unquote,urlsplit
from collection_control.artifacts import record_digest,_unique_object
from collection_control.repository import Conflict,digest


def legal_role(record):
    metadata=record.get('metadata',{})
    if metadata.get('content_role')=='legal_disclosure':return 'legal_disclosure'
    path=unquote(urlsplit(metadata.get('provenance',{}).get('canonical_uri','')).path).lower()
    filename=path.rsplit('/',1)[-1]
    if re.search(r'\.(pdf|docx?|txt|md)$',filename) and re.search(r'(?:agreement|disclosure|terms[-_ ](?:and[-_ ])?conditions|certificate[-_ ]of[-_ ]insurance)',filename):
        return 'legal_document'
    return 'primary'


_CHUNK_BYTES=65536

class _SnapshotJSON:
    """One-record buffer; digest covers exact bytes consumed, including EOF."""
    def __init__(self,file):
        self.file=file;self.hash=hashlib.sha256();self.buffer='';self.pos=0;self.eof=False
        self.utf8=codecs.getincrementaldecoder('utf-8')();self.decoder=json.JSONDecoder(object_pairs_hook=_unique_object)
    def feed(self):
        raw=self.file.read(_CHUNK_BYTES);self.hash.update(raw);self.eof=not raw
        self.buffer=self.buffer[self.pos:]+self.utf8.decode(raw,final=self.eof);self.pos=0
    def peek(self):
        while True:
            while self.pos<len(self.buffer) and self.buffer[self.pos].isspace():self.pos+=1
            if self.pos<len(self.buffer):return self.buffer[self.pos]
            if self.eof:return ''
            self.feed()
    def take(self,character):
        if self.peek()!=character:raise Conflict('Invalid published snapshot JSON')
        self.pos+=1
    def value(self):
        if not self.peek():raise Conflict('Incomplete published snapshot JSON')
        while True:
            try:value,end=self.decoder.raw_decode(self.buffer,self.pos)
            except json.JSONDecodeError:
                if self.eof:raise Conflict('Invalid published snapshot JSON') from None
                self.feed();continue
            if end==len(self.buffer) and not self.eof:self.feed();continue
            if end<len(self.buffer) and self.buffer[end] not in ' \t\r\n,:]}':raise Conflict('Invalid published snapshot JSON token')
            self.pos=end;return value

@lru_cache(maxsize=4)
def snapshot_legal_hashes(root,sha):
    """Classify published evidence without re-admission or full vectors in RAM.

    Return only after verifying the complete stream hash. Writer admission stays
    strict and uses load_artifact separately.
    """
    digest(sha);path=Path(root).resolve()/(sha+'.json');result=set();seen=set()
    try:
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        with os.fdopen(fd,'rb') as file:
            info=os.fstat(file.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size>512*1024*1024:raise Conflict('Invalid or oversized artifact file')
            reader=_SnapshotJSON(file);reader.take('{')
            if reader.peek()!='}':
                while True:
                    key=reader.value()
                    if not isinstance(key,str) or key in seen:raise Conflict('Invalid or duplicate snapshot field')
                    seen.add(key);reader.take(':')
                    if key=='records':
                        reader.take('[')
                        if reader.peek()!=']':
                            while True:
                                record=reader.value()
                                if not isinstance(record,dict):raise Conflict('Invalid published snapshot record')
                                if legal_role(record)!='primary':result.add(record_digest(record))
                                if reader.peek()==']':break
                                reader.take(',')
                        reader.take(']')
                    else:reader.value()
                    if reader.peek()=='}':break
                    reader.take(',')
            reader.take('}')
            if reader.peek() or 'records' not in seen:raise Conflict('Invalid published snapshot JSON')
            if reader.hash.hexdigest()!=sha:raise Conflict('Artifact SHA-256 mismatch')
    except OSError as error:raise Conflict('Artifact unavailable or symlink refused') from error
    except (UnicodeError,ValueError,TypeError,KeyError) as error:raise Conflict('Invalid published snapshot JSON') from error
    return frozenset(result)
