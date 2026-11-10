"""Exact content/config reuse. Cache entries grant no visibility or source identity."""
import hashlib,math


def validate_vector(vector):
    if not isinstance(vector,list) or len(vector)!=2048 or any(isinstance(x,bool) or not isinstance(x,(int,float)) or not math.isfinite(x) for x in vector):raise ValueError('Legal cache requires a complete finite2048vector')
    if abs(math.sqrt(sum(x*x for x in vector))-1)>1e-4:raise ValueError('Legal cache vector violates unit normalization')
    return vector


class LegalEmbeddingCache:
    def __init__(self,pool):self.pool=pool
    async def get(self,contract,texts):
        if not texts:return {}
        rows=await self.pool.fetch('SELECT e.content_sha256,c.text,e.vector FROM application.legal_text_embeddings e JOIN application.legal_text_content c USING(content_sha256) WHERE e.embedding_contract_sha256=$1 AND e.content_sha256=ANY($2::text[])',contract,list(texts))
        result={}
        for row in rows:
            sha=row['content_sha256']
            if hashlib.sha256(row['text'].encode()).hexdigest()!=sha or row['text']!=texts[sha]:raise ValueError('Legal cached content does not match its complete requested text')
            result[sha]=validate_vector(list(row['vector']))
        return result
    async def put(self,contract,text,vector):
        validate_vector(vector);sha=hashlib.sha256(text.encode()).hexdigest()
        async with self.pool.acquire() as c,c.transaction():
            await c.execute('INSERT INTO application.legal_text_content(content_sha256,text) VALUES($1,$2) ON CONFLICT DO NOTHING',sha,text)
            actual=await c.fetchval('SELECT text FROM application.legal_text_content WHERE content_sha256=$1',sha)
            if actual!=text:raise ValueError('Legal cached text differs despite matching hash')
            await c.execute('INSERT INTO application.legal_text_embeddings(embedding_contract_sha256,content_sha256,vector) VALUES($1,$2,$3) ON CONFLICT DO NOTHING',contract,sha,vector)
            stored=await c.fetchval('SELECT vector FROM application.legal_text_embeddings WHERE embedding_contract_sha256=$1 AND content_sha256=$2',contract,sha)
            return validate_vector(list(stored))
