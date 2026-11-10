"""Infrastructure-only independent generation writer, with no source/model calls."""
import asyncio
import hashlib

from .artifacts import canonical, content_digest, load_artifact
from .repository import Conflict, Fenced


class GenerationWriter:
    def __init__(self,repository,artifact_root,stores,*,allow_synthetic=False):
        self.repository=repository
        self.artifact_root=artifact_root
        self.stores=stores
        self.allow_synthetic=allow_synthetic

    async def run(self,operation_id,*,ttl_seconds=1800):
        lease=await self.repository.claim(operation_id,ttl_seconds=ttl_seconds)
        if lease is None:return False
        work=asyncio.create_task(self._write(lease))
        heartbeat=asyncio.create_task(self._heartbeat(lease,ttl_seconds))
        try:
            done,_=await asyncio.wait([work,heartbeat],return_when=asyncio.FIRST_COMPLETED)
            if work in done:return await work
            # Normal heartbeat completion means this same attempt published.
            # Renewal failure instead cancels our work; native thread effects
            # may finish only in the abandoned attempt's isolated generation.
            await heartbeat
            return await work
        except BaseException:
            release=getattr(self.repository,'release_retry',None)
            if release:
                try:await asyncio.shield(release(lease))
                except Fenced:pass
            raise
        finally:
            for task in (work,heartbeat):
                if not task.done():task.cancel()
            await asyncio.gather(work,heartbeat,return_exceptions=True)

    async def _heartbeat(self,lease,ttl):
        while True:
            await asyncio.sleep(min(30,ttl/3))
            if not await self.repository.renew(lease,ttl_seconds=ttl):return

    async def _write(self,lease):
        try:
            artifact=await asyncio.to_thread(load_artifact,self.artifact_root,lease['artifact_sha256'])
            if hashlib.sha256(canonical(artifact['contract'])).hexdigest()!=lease['contract_sha256']:
                raise Conflict('Artifact embedding/schema contract differs from collection')
            requires_proof=any(r['metadata']['origin']!='synthetic' or 'provenance' in r['metadata'] or 'durable_document_id' in r['metadata'] for r in artifact['records'])
            # Empty generations can be strictly admitted unlink operations.
            if not artifact['records']:
                requires_proof=callable(getattr(self.repository,'verify_admission',None))
            if requires_proof:
                verifier=getattr(self.repository,'verify_admission',None)
                if verifier is None:raise Conflict('Grounded source publication requires the admitted-stage provenance bridge')
                await verifier(lease,artifact)
            elif not self.allow_synthetic:
                raise Conflict('Synthetic publication requires explicit qualification opt-in')
        except Conflict:
            # Permanent admission rejection has no backend effects. Preserve
            # prior publication/history, and unblock a corrected new intent.
            try:await self.repository.cancel(lease,'artifact_admission_rejected')
            except Fenced:pass
            raise
        expected=(content_digest(artifact['records']),len(artifact['records']))
        for target in lease['targets']:
            if target not in self.stores:raise Conflict('Required backend adapter is unavailable')
            actual=await self.stores[target].write_verify(lease['handles'][target],artifact)
            if actual!=expected:raise Conflict('Backend visibility/content verification differs from artifact')
            await self.repository.receipt(lease,target,*actual)
        await self.repository.publish(lease)
        return True
